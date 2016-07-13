// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

#define WIN32_LEAN_AND_MEAN

//#include "stdafx.h"
#include <windows.h>
#include <winsock2.h>
#include <mswsock.h>
#include <mstcpip.h>
#include "RIOSock.h"
#include <cstdlib>
#include "Locks.h"
#include <new>
#include <cassert>

// Need to link with Ws2_32.lib
#pragma comment (lib, "Ws2_32.lib")

const DWORD DefaultRIOCQSize = 256;
DWORD CQSize = 0;
DWORD CQUsed = 0;

LONG RIOSockRef;
PrioritizedLock *CQAccessLock = nullptr;
RIO_CQ CompletionQueue = nullptr;
RIO_RQ RequestQueue = nullptr;
RIO_NOTIFICATION_COMPLETION CompletionType;
RIO_EXTENSION_FUNCTION_TABLE RIOFuncs = { 0 };

//
// Local Functions
//

HRESULT EnsureWinSockMethods(_In_ SOCKET socket);
RIO_CQ CreateRIOCompletionQueue(_In_ DWORD queueSize, _In_opt_ PRIO_NOTIFICATION_COMPLETION pNotificationCompletion);
void CloseRIOCompletionQueue(_In_ RIO_CQ cq);
ULONG DequeueRIOCompletion(_In_ RIO_CQ cq, _Out_writes_to_(arraySize, return) PRIORESULT array, _In_ ULONG arraySize);
BOOL ResizeRIOCompletionQueue(_In_ RIO_CQ cq, _In_ ULONG queueSize);

//+
//  Function:
//      EnsureWinSockMethods()
//
//  Description:
//      Static function only to be called locally to ensure WSAStartup is held
//      for the function pointers to remain accurate
//
//  Result:
//      Returns a registered buffer descriptor, if no errors occurs.
//      Otherwise, a value of RIO_INVALID_BUFFERID is returned.
//-
LONG WinSockMethodsLock = 0;
HRESULT EnsureWinSockMethods(
    _In_  SOCKET  socket
    )
{
    static const LONG LockUninitialized = 0;
    static const LONG LockInitialized = 1;
    static const LONG LockInitializing = 2;

    LONG lastState;

    while((lastState = ::InterlockedCompareExchange(
                &WinSockMethodsLock,
                LockInitializing,
                LockUninitialized)) == LockInitializing)
    {
        Sleep(0);
    }
    
    if (lastState == LockInitialized)
    {
        return S_OK;
    }

    WSADATA wsaData;
    auto err = WSAStartup(WINSOCK_VERSION, &wsaData);
    if (err != 0) {
        // Reset lock to uninitialized
        ::InterlockedExchange(&WinSockMethodsLock, LockUninitialized);
        // WSAStartup does not set LastWin32Error
        SetLastError(err);
        return HRESULT_FROM_WIN32(err);
    }

    // Check to see if we need to create a temp socket
    auto localSocket = socket;
    if (INVALID_SOCKET == localSocket)
    {
        DWORD dwFlags = WSA_FLAG_NO_HANDLE_INHERIT | WSA_FLAG_OVERLAPPED | WSA_FLAG_REGISTERED_IO;
        localSocket = WSASocket(AF_INET, SOCK_STREAM, IPPROTO_TCP, nullptr, 0, dwFlags);
        if (INVALID_SOCKET == localSocket)
        {
            DWORD errorCode = WSAGetLastError();
            // Reset lock to uninitialized
            WSACleanup();
            ::InterlockedExchange(&WinSockMethodsLock, LockUninitialized);
            SetLastError(errorCode);
            return HRESULT_FROM_WIN32(errorCode);
        }
    }

    GUID funcGuid = WSAID_MULTIPLE_RIO;
    DWORD dwBytes = 0;

    if (WSAIoctl(
            localSocket,
            SIO_GET_MULTIPLE_EXTENSION_FUNCTION_POINTER,
            &funcGuid, sizeof(GUID),
            &RIOFuncs, sizeof(RIOFuncs),
            &dwBytes,nullptr, nullptr) != 0)
    {
        DWORD errorCode = WSAGetLastError();
        if (localSocket != socket)
        {
            closesocket(localSocket);
        }

        WSACleanup();
        // Reset lock to uninitialized
        ::InterlockedExchange(&WinSockMethodsLock, LockUninitialized);
        SetLastError(errorCode);
        return HRESULT_FROM_WIN32(errorCode);
    }

    // Update lock to fully Initialized
    ::InterlockedExchange(&WinSockMethodsLock, LockInitialized);
    if (localSocket != socket) {
        closesocket(localSocket);
    }
    return S_OK;
}


//+
//  Function:
//      CreateRIOCompletionQueue()
//
//  Description:
//      Internally, this function calls RIOCreateCompletionQueue to
//      create a completion queue.
//
//  Result:
//      Returns a RIO_CQ
//-
FORCEINLINE
RIO_CQ CreateRIOCompletionQueue(
    _In_     DWORD                         queueSize,
    _In_opt_ PRIO_NOTIFICATION_COMPLETION  pNotificationCompletion
    )
{
    auto hr = EnsureWinSockMethods(INVALID_SOCKET);
    if (FAILED(hr))
    {
        return RIO_INVALID_CQ;
    }
    return RIOFuncs.RIOCreateCompletionQueue(queueSize, pNotificationCompletion);
}


//+
//  Function:
//      CloseRIOCompletionQueue()
//
//  Description:
//      Internally, this function calls RIOCloseCompletionQueue to
//      close a completion queue.
//
//  Result:
//      None.
//-
FORCEINLINE
void CloseRIOCompletionQueue(
    _In_ RIO_CQ cq
    )
{
    auto hr = EnsureWinSockMethods(INVALID_SOCKET);
    if (FAILED(hr))
    {
        return;
    }
    RIOFuncs.RIOCloseCompletionQueue(cq);
}


//+
//  Function:
//      DequeueRIOCompletion()
//
//  Description:
//      Internally, this function calls RIODequeueCompletion to
//      remove entries from an I/O completion queue.
//
//  Result:
//      Returns the number of completion entries removed from the specified completion queue. 
//-
FORCEINLINE
ULONG DequeueRIOCompletion(
    _In_                                RIO_CQ      cq,
    _Out_writes_to_(arraySize, return)  PRIORESULT  array,
    _In_                                ULONG       arraySize
    )
{
    auto hr = EnsureWinSockMethods(INVALID_SOCKET);
    if (FAILED(hr))
    {
        return RIO_CORRUPT_CQ;
    }

    return RIOFuncs.RIODequeueCompletion(cq, array, arraySize);
}


//+
//  Function:
//      ResizeRIOCompletionQueue()
//
//  Description:
//      Internally, this function calls RIOResizeCompletionQueue to
//      resizes the I/O completion queue.
//
//  Result:
//      None.
//-
FORCEINLINE
BOOL ResizeRIOCompletionQueue(
    _In_  RIO_CQ cq,
    _In_  ULONG queueSize
    )
{
    auto hr = EnsureWinSockMethods(INVALID_SOCKET);
    if (FAILED(hr))
    {
        return FALSE;
    }
    return RIOFuncs.RIOResizeCompletionQueue(cq, queueSize);
}


//
// Global APIs
//


//+
//  Function:
//      RIOSockInitialize()
//
//  Description:
//      This function is global initializer for RIOSock.dll and must be called
//      before any RIOSock APIs are invoked.
//
//  Result:
//      HRESULT codes.
//-
HRESULT RIOSOCKAPI RIOSockInitialize()
{
    // Return if already initialized
    if (RIOSockRef > 0) {
        InterlockedIncrement(&RIOSockRef);
        return S_OK;
    }

    // Create lock for Completion Queue access
    CQAccessLock = new (std::nothrow) PrioritizedLock;
    if (nullptr == CQAccessLock) {
        return E_OUTOFMEMORY;
    }

    // Create IOCP handle
    auto iocpHandle = CreateIoCompletionPort(INVALID_HANDLE_VALUE, nullptr, 0, 0);
    if (iocpHandle == nullptr)
    {
        DWORD errorCode = GetLastError();
        delete CQAccessLock;
        SetLastError(errorCode);
        return HRESULT_FROM_WIN32(errorCode);
    }

    // Create OVERLAPPED
    auto overLapped = calloc(1, sizeof(OVERLAPPED));
    if (nullptr == overLapped) {
        // Close IOCP handle
        CloseHandle(iocpHandle);
        delete CQAccessLock;
        SetLastError(WSAENOBUFS);
        return HRESULT_FROM_WIN32(WSAENOBUFS);
    }

    // With RIO, we don't associate the IOCP handle with the socket like 'typical' sockets
    // - Instead we directly pass the IOCP handle through RIOCreateCompletionQueue
    ::ZeroMemory(&CompletionType, sizeof(CompletionType));

    CompletionType.Type = RIO_IOCP_COMPLETION;
    CompletionType.Iocp.CompletionKey = reinterpret_cast<void*>(1); 
    CompletionType.Iocp.Overlapped = overLapped;
    CompletionType.Iocp.IocpHandle = iocpHandle;
    
    // Create a completion queue
    CompletionQueue = CreateRIOCompletionQueue(DefaultRIOCQSize, &CompletionType);
    if (RIO_INVALID_CQ == CompletionQueue) {
        DWORD errorCode = WSAGetLastError();
        CloseHandle(iocpHandle);
        free(overLapped);
        delete CQAccessLock;
        SetLastError(errorCode);
        return HRESULT_FROM_WIN32(errorCode);
    }

    // now that the CQ is created, update info
    CQSize = DefaultRIOCQSize;
    CQUsed = 0;

    return S_OK;
}


//+
//  Function:
//      RIOSockUninitialize()
//
//  Description:
//      This function cleans up resources allocated by RIOSockInitialize.
//
//  Result:
//      None.
//-
void RIOSOCKAPI RIOSockUninitialize()
{
    InterlockedDecrement(&RIOSockRef);
    if (RIOSockRef > 0) return;

    if (CompletionQueue != RIO_INVALID_CQ) {
        CloseRIOCompletionQueue(CompletionQueue);
        CompletionQueue = RIO_INVALID_CQ;
    }

    if (CompletionType.Iocp.IocpHandle != nullptr) {
        CloseHandle(CompletionType.Iocp.IocpHandle);
        CompletionType.Iocp.IocpHandle = nullptr;
    }

    free(CompletionType.Iocp.Overlapped);
    CompletionType.Iocp.Overlapped = nullptr;

    delete CQAccessLock;
    CQAccessLock = nullptr;
}


//+
//  Function:
//      CreateRIOSocket()
//
//  Description:
//      This function creates a socket that bound to a local loop-back for use with RIO.
//
//  Parameters:
//      localAddr     -  A pointer to the beginning of the memory buffer to register.
//      localAddrLen  -  The length, in bytes, in the buffer to register.
//
//  Result:
//      Returns a new socket, if no errors occurs. Otherwise, a value of INVALID_SOCKET is returned.
//-
SOCKET RIOSOCKAPI CreateRIOSocket(
    _Out_    SOCKADDR  *localAddr,
    _Inout_  int       *localAddrLen
    )
{
    // DWORD dwFlags = WSA_FLAG_NO_HANDLE_INHERIT | WSA_FLAG_OVERLAPPED | WSA_FLAG_REGISTERED_IO;
    auto socket = WSASocket(AF_INET, SOCK_STREAM, IPPROTO_TCP, nullptr, 0, WSA_FLAG_REGISTERED_IO);
    if (INVALID_SOCKET == socket)
    {
        DWORD errorCode = WSAGetLastError();
        SetLastError(errorCode);
        return INVALID_SOCKET;
    }

    // Enables SIO_LOOPBACK_FAST_PATH
    auto OptionValue = 1;
    DWORD NumberOfBytesReturned = 0;
    if (WSAIoctl(socket,
                 SIO_LOOPBACK_FAST_PATH,
                 &OptionValue,
                 sizeof(OptionValue),
                 nullptr, 0,
                 &NumberOfBytesReturned,
                 nullptr, nullptr) == SOCKET_ERROR)
    {
        DWORD errorCode = WSAGetLastError();
        closesocket(socket);
        SetLastError(errorCode);
        return INVALID_SOCKET;
    }

    // Bind socket for exclusive access
    const BOOL bindExclUse = 1;
    if (setsockopt(socket,
        SOL_SOCKET,
        SO_EXCLUSIVEADDRUSE,
        reinterpret_cast<const char *>(&bindExclUse),
        sizeof(bindExclUse)) == SOCKET_ERROR)
    {
        // Unexpected failure: report it then close our socket
        DWORD errorCode = WSAGetLastError();
        closesocket(socket);
        SetLastError(errorCode);
        return INVALID_SOCKET;
    }

    // Bind the socket to the loop-back address
    SOCKADDR_IN sockAddr;
    sockAddr.sin_family = AF_INET;
    sockAddr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    sockAddr.sin_port = htons(0);
    if (bind(socket, reinterpret_cast<SOCKADDR *>(&sockAddr), sizeof(sockAddr)) == SOCKET_ERROR)
    {
        DWORD errorCode = WSAGetLastError();
        closesocket(socket);
        SetLastError(errorCode);
        return INVALID_SOCKET;
    }

    // Retrieve the local name (addr) of the socket
    if (getsockname(socket, localAddr, localAddrLen) == SOCKET_ERROR)
    {
        DWORD errorCode = WSAGetLastError();
        closesocket(socket);
        SetLastError(errorCode);
        return INVALID_SOCKET;
    }

    return socket;
}

//+
//  Function:
//      PostRIOReceive()
//
//  Description:
//      This function posts a receive operation to receives data on a connected RIO socket.
//
//  Parameters:
//      socketQueue      -  The request queue that identifies a connected RIO socket.
//      pData            -  The portion of the registered buffer in which to receive data.
//      dataBufferCount  -  The data buffer count of the buffer pointed to by the pData parameter.
//      flags            -  A set of flags that modify the behavior of the RIOReceive function.
//      requestContext   -  The request context to associate with this receive operation.
//
//  Result:
//      Returns true if no error occurs. Otherwise, a value of false is returned.
//-
FORCEINLINE
BOOL RIOSOCKAPI PostRIOReceive(
    _In_  RIO_RQ    socketQueue,
    _In_  PRIO_BUF  pData,
    _In_  ULONG     dataBufferCount,
    _In_  DWORD     flags,
    _In_  PVOID     requestContext
    )
{
    auto hr = EnsureWinSockMethods(INVALID_SOCKET);
    if (FAILED(hr))
    {
        return FALSE;
    }

    return RIOFuncs.RIOReceive(socketQueue, pData, dataBufferCount, flags, requestContext);
}


//+
//  Function:
//      PostRIOSend()
//
//  Description:
//      This function posts a send operation to send data on a connected RIO socket.
//
//  Parameters:
//      socketQueue      -  The request queue that identifies a connected RIO socket.
//      pData            -  The portion of the registered buffer in which to receive data.
//      dataBufferCount  -  The data buffer count of the buffer pointed to by the pData parameter.
//      flags            -  A set of flags that modify the behavior of the RIOReceive function.
//      requestContext   -  The request context to associate with this receive operation.
//
//  Result:
//      Returns TRUE if no error occurs. Otherwise, a value of FALSE is returned.
//-
FORCEINLINE
BOOL RIOSOCKAPI PostRIOSend(
    _In_  RIO_RQ    socketQueue,
    _In_  PRIO_BUF  pData,
    _In_  ULONG     dataBufferCount,
    _In_  DWORD     flags,
    _In_  PVOID     requestContext
    )
{
    auto hr = EnsureWinSockMethods(INVALID_SOCKET);
    if (FAILED(hr))
    {
        return FALSE;
    }

    return RIOFuncs.RIOSend(socketQueue, pData, dataBufferCount, flags, requestContext);
}


//+
//  Function:
//      RegisterRIONotify()
//
//  Description:
//      This function registers the method to use for notification behavior.
//
//  Result:
//      Returns TRUE if no error occurs. Otherwise, a value of FALSE is returned.
//-
FORCEINLINE
BOOL RIOSOCKAPI RegisterRIONotify()
{
    auto hr = EnsureWinSockMethods(INVALID_SOCKET);
    if (FAILED(hr))
    {
        return FALSE;
    }

    auto notify = RIOFuncs.RIONotify(CompletionQueue);
    if (notify != ERROR_SUCCESS) {
        SetLastError(notify);
        return FALSE;
    }
    return TRUE;
}


//+
//  Function:
//      RegisterRIOBuffer()
//
//  Description:
//      This function registers a specified buffer for use with RIO Socket.
//
//  Parameters:
//      dataBuffer  -  A pointer to the beginning of the memory buffer to register.
//      dataLength  -  The length, in bytes, in the buffer to register.
//
//  Result:
//      Returns a registered buffer descriptor, if no errors occurs.
//      Otherwise, a value of RIO_INVALID_BUFFERID is returned.
//-
FORCEINLINE
RIO_BUFFERID RIOSOCKAPI RegisterRIOBuffer(
    _In_  PCHAR  dataBuffer,
    _In_  DWORD  dataLength
)
{
    auto hr = EnsureWinSockMethods(INVALID_SOCKET);
    if (FAILED(hr))
    {
        return RIO_INVALID_BUFFERID;
    }

    return RIOFuncs.RIORegisterBuffer(dataBuffer, dataLength);
}


//+
//  Function:
//      DeregisterRIOBuffer()
//
//  Description:
//      This function deregisters a registered buffer used with RIO socket.
//
//  Parameters:
//      bufferId  -  A descriptor identifying a registered buffer.
//
//  Result:
//      None.
//-
FORCEINLINE
void RIOSOCKAPI DeregisterRIOBuffer(
    _In_ RIO_BUFFERID bufferId
)
{
    auto hr = EnsureWinSockMethods(INVALID_SOCKET);
    if (FAILED(hr))
    {
        return;
    }
    return RIOFuncs.RIODeregisterBuffer(bufferId);
}


//+
//  Function:
//      AllocateRIOCompletion()
//
//  Description:
//      This function makes rooms in the CQ for a new IO.
//       - check if there is room in the CQ for the new IO
//       - if not, take a writer lock around the CS to halt readers and to write to cq_used
//         then take the CS over the CQ, and resize the CQ by 1.5 times current size
//
//  Parameters:
//      numCompltetion  -  The number of completions that need to be allocated from the completion queue.
//
//  Result:
//      return TRUE, if no error occurs; otherwise, FALSE.
//-
FORCEINLINE
BOOL RIOSOCKAPI AllocateRIOCompletion(
    _In_ DWORD numCompltetion
    )
{
    // Taking an priority lock to interrupt the general lock taken by the deque IO path
    // We want to interrupt the IO path so we can initiate more IO if we need to grow the CQ
    AutoReleasePriorityLock priorityLock(*CQAccessLock);

    auto newCQUsed = CQUsed + numCompltetion;
    auto newCQSize = CQSize; // not yet resized
    if (CQSize < newCQUsed) {
        if (RIO_MAX_CQ_SIZE == CQSize || newCQUsed > RIO_MAX_CQ_SIZE)
        {
            // fail hard if we are already at the max CQ size and can't grow it for more IO
            return FALSE;
        }

        // multiply newCQUsed by 1.25 for better growth patterns
        newCQSize = static_cast<DWORD>(newCQUsed * 1.25);
        if (newCQSize > RIO_MAX_CQ_SIZE) {
            static_assert(MAXLONG / 1.25 > RIO_MAX_CQ_SIZE, "CQSize can overflow");
            newCQSize = RIO_MAX_CQ_SIZE;
        }

        if (!ResizeRIOCompletionQueue(CompletionQueue, newCQSize))
        {
            return FALSE;
        }
    }

    // update CQUsed and CQSize on the success path
    CQUsed = newCQUsed;
    CQSize = newCQSize;
    return TRUE;
}


//+
//  Function:
//      ReleaseRIOCompletion()
//
//  Description:
//      This function release rooms back to the CQ
//
//  Parameters:
//      numCompltetion  -  The number of completions that need to be released.
//
//  Result:
//      return FALSE, if numCompltetion > CQUsed; otherwise, TRUE.
//-
FORCEINLINE
BOOL RIOSOCKAPI ReleaseRIOCompletion(
    _In_ DWORD numCompletion 
    )
{
    AutoReleasePriorityLock priorityLock(*CQAccessLock);
    if (CQUsed < numCompletion)
    {
        return FALSE;
    }

    CQUsed -= numCompletion;
    return TRUE;
}


//+
//  Function:
//      DequeueRIOResults()
//
//  Description:
//      This function dequeue RIO results from the I/O completion queue used with RIO socket.
//      It will always post a Notify with proper synchronization.
//
//  Parameters:
//      rioResults     -  An array of RIORESULT structures to receive the description of the completions dequeued.
//      rioResultSize  -  The maximum number of entries in the rioResults to write.
//
//  Result:
//      If no error occurs, it returns the number of RIO results retrieved from the completion queue.
//      Otherwise, a value of RIO_CORRUPT_CQ is returned to indicate that the state of the completion
//      queue has become corrupt due to memory corruption or misuse of the RIO functions.
//-
FORCEINLINE
DWORD RIOSOCKAPI DequeueRIOResults(
    _Out_  PRIORESULT  rioResults,
    _In_   DWORD       rioResultSize
    )
{
    // Taking a lower-priority lock, to allow the priority lock to interrupt
    // dequeuing. So it can add space to the CQ
    AutoReleaseDefaultLock defaultLock(*CQAccessLock);

    auto resultCount = DequeueRIOCompletion(CompletionQueue, rioResults, rioResultSize);
    if (0 == resultCount || RIO_CORRUPT_CQ == resultCount)
    {
        // We were notified there were completions, but we can't dequeue any IO
        // Something has gone horribly wrong - likely our CQ is corrupt.
        return resultCount;
    }

    // Immediately after invoking Dequeue, post another Notify
    auto notifyResult = RegisterRIONotify();
    if (notifyResult == FALSE)
    {
        // if notify fails, we can't reliably know when the next IO completes
        // this will cause everything to come to a grinding halt
        return RIO_CORRUPT_CQ;
    }

    return resultCount;
}


//+
//  Function:
//      CreateRIORequestQueue()
//
//  Description:
//      This function creates a request queue by calling RIOCreateRequestQueue.
//
//  Parameters:
//      socket                 -  A socket to for the new request queue.
//      maxOutstandingReceive  -  The maximum number of outstanding receives allowed on the socket.
//      maxOutstandingSend     -  The maximum number of outstanding sends allowed on the socket.
//      socketContext          -  The socket context to associate with this request queue.
//
//  Result:
//      If no error occurs, it returns a new request queue. Otherwise, a value of RIO_INVALID_RQ is returned.
//-
FORCEINLINE
RIO_RQ RIOSOCKAPI CreateRIORequestQueue(
    _In_ SOCKET  socket,
    _In_ ULONG   maxOutstandingReceive,
    _In_ ULONG   maxOutstandingSend,
    _In_ PVOID   socketContext
    )
{
    // A request queue is associated with a socket, ensure that the client passed us a valid socket 
    assert(socket != INVALID_SOCKET);

    auto hr = EnsureWinSockMethods(socket);
    if (FAILED(hr))
    {
        return RIO_INVALID_RQ;
    }

    return RIOFuncs.RIOCreateRequestQueue(
        socket,
        maxOutstandingReceive,
        1,
        maxOutstandingSend,
        1,
        CompletionQueue,
        CompletionQueue,
        socketContext
    );
}


//+
//  Function:
//      ResizeRIORequestQueue()
//
//  Description:
//      This function resizes a request queue by calling RIOResizeRequestQueue.
//
//  Parameters:
//      rq                     -  A request queue to be resize.
//      maxOutstandingReceive  -  The maximum number of outstanding receives allowed on the socket.
//      maxOutstandingSend     -  The maximum number of outstanding sends allowed on the socket.
//
//  Result:
//      If no error occurs, it returns TRUE. Otherwise, a value of FALSE is returned.
//-
BOOL RIOSOCKAPI ResizeRIORequestQueue(
    _In_ RIO_RQ rq,
    _In_ DWORD  maxOutstandingReceive,
    _In_ DWORD  maxOutstandingSend
)
{
    // ensure that the client passed us a valid RQ 
    assert(rq != RIO_INVALID_RQ);

    auto hr = EnsureWinSockMethods(INVALID_SOCKET);
    if (FAILED(hr))
    {
        return FALSE;
    }

    return RIOFuncs.RIOResizeRequestQueue(rq, maxOutstandingReceive, maxOutstandingSend);
}


//+
//  Function:
//      GetRIOCompletionStatus()
//
//  Description:
//      This function calls GetQueuedCompletionStatus() internally to dequeue an IO completion packet.
//      If there is no completion packet queued, the function blocks the thread.
//
//  Result:
//      If no error occurs, it returns a new request queue. Otherwise, a value of RIO_INVALID_RQ is returned.
//-
FORCEINLINE
BOOL RIOSOCKAPI GetRIOCompletionStatus()
{
    DWORD bytesTransferred;
    ULONG_PTR completionKey;
    OVERLAPPED *pov = nullptr;


    if (!GetQueuedCompletionStatus(
            CompletionType.Iocp.IocpHandle,
            &bytesTransferred,
            &completionKey,
            &pov,
            INFINITE))
    {
        auto lastError = GetLastError();
        SetLastError(lastError);
        return FALSE;
    }

    return TRUE;
}


//////////////////////////////////////////////////////////////////////////


//+
// DLL Entry
//-
BOOL APIENTRY DllMain(HMODULE hModule, DWORD dwReason, LPVOID lpReserved)
{
    UNREFERENCED_PARAMETER(lpReserved);
    if (dwReason == DLL_PROCESS_ATTACH)
    {
        // Initializes use of Winsock 2 DLL
        WSADATA wsaData;
        if (WSAStartup(WINSOCK_VERSION, &wsaData) != 0)
        {
            return FALSE;
        }

        // Disables the DLL_THREAD_ATTACH and DLL_THREAD_DETACH notifications 
        DisableThreadLibraryCalls(hModule);
        return TRUE;
    }

    if (dwReason == DLL_PROCESS_DETACH)
    {
        // Terminates use of the Winsock 2 DLL
        WSACleanup();
    }

    return TRUE;
}