// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

#pragma once

#ifdef __cplusplus
extern "C" {
#endif
    
#ifdef RIOSOCK_DLL
#define RIOSOCKAPI __declspec(dllexport) __stdcall
#else
#define RIOSOCKAPI __declspec(dllimport) __stdcall
#endif

    //
    // Global Routings
    //

    // Global initializer for RIOSock.dll
    HRESULT RIOSOCKAPI RIOSockInitialize();

    // Cleans up resources allocated by RIOSockInitialize.
    void RIOSOCKAPI RIOSockUninitialize();

    //
    // Operations
    //

    // Creates a socket that bound to a local loop-back for use with RIO.
    SOCKET RIOSOCKAPI CreateRIOSocket(
        _Out_   SOCKADDR  *localAddr,
        _Inout_ int       *addrLen
        );

    // Posts a receive operation to receives data on a connected RIO socket.
    BOOL RIOSOCKAPI PostRIOReceive(
        _In_  RIO_RQ    socketQueue,
        _In_  PRIO_BUF  pData,
        _In_  ULONG     dataBufferCount,
        _In_  DWORD     flags,
        _In_  PVOID     requestContext
        );

    // Posts a send operation to send data on a connected RIO socket.
    BOOL RIOSOCKAPI PostRIOSend(
        _In_  RIO_RQ    socketQueue,
        _In_  PRIO_BUF  pData,
        _In_  ULONG     dataBufferCount,
        _In_  DWORD     flags,
        _In_  PVOID     requestContext
        );

    // Registers the method to use for notification behavior.
    BOOL RIOSOCKAPI RegisterRIONotify();
    
    // Registers a specified buffer for use with RIO Socket
    RIO_BUFFERID RIOSOCKAPI RegisterRIOBuffer(
        _In_  PCHAR  dataBuffer,
        _In_  DWORD  dataLength
        );

    // Deregisters a registered buffer used with RIO socket.
    void RIOSOCKAPI DeregisterRIOBuffer(
        _In_  RIO_BUFFERID  bufferId
        );

    // Makes rooms in the completion queue for a new IO.
    BOOL RIOSOCKAPI AllocateRIOCompletion(
        _In_  DWORD  numCompltetion
        );

    // Release rooms in the completion queue
    BOOL RIOSOCKAPI ReleaseRIOCompletion(
        _In_  DWORD  numCompletion
        );

    // Dequeues RIO results from the I/O completion queue used with RIO socket.
    DWORD RIOSOCKAPI DequeueRIOResults(
        _Out_  PRIORESULT  rioResults,
        _In_   DWORD       rioResultSize
        );

    // Creates a request queue
    RIO_RQ RIOSOCKAPI CreateRIORequestQueue(
        _In_ SOCKET  socket,
        _In_ ULONG   maxOutstandingReceive,
        _In_ ULONG   maxOutstandingSend,
        _In_ PVOID   socketContext
        );

    // Resizes a request queue
    BOOL RIOSOCKAPI ResizeRIORequestQueue(
        _In_ RIO_RQ rq,
        _In_ DWORD  maxOutstandingReceive,
        _In_ DWORD  maxOutstandingSend
        );

    // Dequeues an IO completion packet
    BOOL RIOSOCKAPI GetRIOCompletionStatus();

#ifdef __cplusplus
}
#endif