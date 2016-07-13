// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

#pragma once

#include <windows.h>

class PrioritizedLock {
public:
    PrioritizedLock() throw()
    {
        InitializeSRWLock(&srwlock);
        InitializeCriticalSectionEx(&cs, 4000, 0);
    }

    ~PrioritizedLock() throw()
    {
        DeleteCriticalSection(&cs);
    }

    // taking an *exclusive* lock to interrupt the *shared* lock taken by the deque IO path
    // - we want to interrupt the IO path so we can initiate more IO if we need to grow the CQ
    _Acquires_exclusive_lock_(this->srwlock)
    _Acquires_lock_(this->cs)
    void PriorityLock() throw()
    {
        AcquireSRWLockExclusive(&srwlock);
        EnterCriticalSection(&cs);
    }

    _Releases_lock_(this->cs)
    _Releases_exclusive_lock_(this->srwlock)
    void PriorityRelease() throw()
    {
        LeaveCriticalSection(&cs);
        ReleaseSRWLockExclusive(&srwlock);
    }

    _Acquires_shared_lock_(this->srwlock)
    _Acquires_lock_(this->cs)
    void DefaultLock() throw()
    {
        AcquireSRWLockShared(&srwlock);
        EnterCriticalSection(&cs);
    }

    _Releases_lock_(this->cs)
    _Releases_shared_lock_(this->srwlock)
    void DefaultRelease() throw()
    {
        LeaveCriticalSection(&cs);
        ReleaseSRWLockShared(&srwlock);
    }

    /// not copyable
    PrioritizedLock(const PrioritizedLock&) = delete;
    PrioritizedLock& operator=(const PrioritizedLock&) = delete;

private:
    SRWLOCK srwlock;
    CRITICAL_SECTION cs;
};

class AutoReleasePriorityLock {
public:
    explicit AutoReleasePriorityLock(PrioritizedLock &priorityLock) throw()
        : prioritizedLock(priorityLock)
    {
        prioritizedLock.PriorityLock();
    }

    ~AutoReleasePriorityLock() throw()
    {
        prioritizedLock.PriorityRelease();
    }

    /// no default ctor
    AutoReleasePriorityLock() = delete;
    /// non-copyable
    AutoReleasePriorityLock(const AutoReleasePriorityLock&) = delete;
    AutoReleasePriorityLock operator=(const AutoReleasePriorityLock&) = delete;

private:
    PrioritizedLock &prioritizedLock;
};


class AutoReleaseDefaultLock {
public:
    explicit AutoReleaseDefaultLock(PrioritizedLock &priorityLock) throw() 
        : prioritizedLock(priorityLock)
    {
        prioritizedLock.DefaultLock();
    }

    ~AutoReleaseDefaultLock() throw()
    {
        prioritizedLock.DefaultRelease();
    }

    /// no default ctor
    AutoReleaseDefaultLock() = delete;
    /// non-copyable
    AutoReleaseDefaultLock(const AutoReleaseDefaultLock&) = delete;
    AutoReleaseDefaultLock operator=(const AutoReleaseDefaultLock&) = delete;

private:
    PrioritizedLock &prioritizedLock;
};