using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace MyReadWriteLock {

    internal class ReaderWriterCount {
        public long lockID;
        public int readercount;
        public int writercount;
        public int upgradecount;
        public ReaderWriterCount next;
    }

    internal class TimeoutTracker {
        private int m_total;
        private int m_start;

        public TimeoutTracker(int millisecondsTimeout) {
            if (millisecondsTimeout < -1)
                throw new ArgumentOutOfRangeException("millisecondsTimeout");
            m_total = millisecondsTimeout;
            if (m_total != -1 && m_total != 0)
                m_start = Environment.TickCount;
            else
                m_start = 0;
        }

        public int RemainingMilliseconds {
            get {
                if (m_total == -1 || m_total == 0)
                    return m_total;

                int elapsed = Environment.TickCount - m_start;
                // elapsed may be negative if TickCount has overflowed by 2^31 milliseconds.  
                if (elapsed < 0 || elapsed >= m_total)
                    return 0;
                return m_total - elapsed;
            }
        }

        public bool IsExpired {
            get {
                return RemainingMilliseconds == 0;
            }
        }
    }

    class MyReadWriteLock : IDisposable {

        int myLock;

        //The variables controlling spinning behavior of Mylock(which is a spin-lock)  自旋行为
        const int LockSpinCycles = 20;
        const int LockSpinCount = 10;
        const int LockSleep0Count = 5;

        // These variables allow use to avoid Setting events (which is expensive) if we don't have to.  
        uint numWriteWaiters; // maximum number of threads that can be doing a WaitOne on the writeEvent  
        uint numReadWaiters; // maximum number of threads that can be doing a WaitOne on the readEvent  
        uint numWriteUpgradeWaiters; // maximum number of threads that can be doing a WaitOne on the upgradeEvent (at most 1).  
        uint numUpgradeWaiters;

        //Variable used for quick check when there are no waiters.  
        bool fNoWaiters;

        int upgradeLockOwnerId;
        int writeLockOwnerId;

        // conditions we wait on.  
        EventWaitHandle writeEvent; // threads waiting to acquire a write lock go here.  
        EventWaitHandle readEvent; // threads waiting to acquire a read lock go here (will be released in bulk)  
        EventWaitHandle upgradeEvent; // thread waiting to acquire the upgrade lock  
        EventWaitHandle waitUpgradeEvent; // thread waiting to upgrade from the upgrade lock to a write lock go here (at most one)  

        // Every lock instance has a unique ID, which is used by ReaderWriterCount to associate itself with the lock  
        // without holding a reference to it.  
        // 每个锁实例都有一个唯一的ID，ReaderWriterCount使用该ID将自身与锁相关联，而不保留对其的引用。
        static long s_nextLockID;
        long lockID;

        // See comments on ReaderWriterCount.  
        [ThreadStatic]
        static ReaderWriterCount t_rwc;

        private const int MaxSpinCount = 20;

        // 32位分成四部分： 1+1+1+29
        uint owners;

        private const uint WRITER_HELD = 0x80000000;
        private const uint WAITING_WRITERS = 0x40000000;
        private const uint WAITING_UPGRADER = 0x20000000;

        private const uint MAX_READER = 0x10000000 - 2;
        private const uint READER_MASK = 0x10000000 - 1;

        public MyReadWriteLock() {
            InitializeThreadCounts();
            fNoWaiters = true;
            lockID = Interlocked.Increment(ref s_nextLockID);
        }

        private void InitializeThreadCounts() {
            upgradeLockOwnerId = -1;
            writeLockOwnerId = -1;
        }

        public void EnterReadLock() {
            TryEnterReadLock(-1);
        }

        public bool TryEnterReadLock(int millisecondsTimeout) {
            return TryEnterReadLock(new TimeoutTracker(millisecondsTimeout));
        }

        private bool TryEnterReadLock(TimeoutTracker timeout) {
            bool result = false;
            try {
                result = TryEnterReadLockCore(timeout);
            }
            finally { }
            return result;
        }

        private bool TryEnterReadLockCore(TimeoutTracker timeout) {

            ReaderWriterCount lrwc = null;
            int id = Thread.CurrentThread.ManagedThreadId;

            if (id == writeLockOwnerId) {
                //Check for AW->AR  
                throw new LockRecursionException("Not allow read after write");
            }
            EnterMyLock();
            lrwc = GetThreadRWCount(false);

            //Check if the reader lock is already acquired. Note, we could check the presence of a reader by not allocating rwc (But that would lead to two lookups in the common case. It's better to keep a count in the struucture).  
            if (lrwc.readercount > 0) {
                ExitMyLock();
                throw new Exception("Not allow recursive read");
            }
            else if (id == upgradeLockOwnerId) {
                //The upgrade lock is already held.Update the global read counts and exit.  

                lrwc.readercount++;
                owners++;
                ExitMyLock();
                return true;
            }

            bool retVal = true;
            int spincount = 0;

            for (; ; ) {
                // We can enter a read lock if there are only read-locks have been given out and a writer is not trying to get in.  
                if (owners < MAX_READER) {
                    // Good case, there is no contention, we are basically done  
                    owners++; // Indicate we have another reader  
                    lrwc.readercount++;
                    break;
                }

                if (spincount < MaxSpinCount) {
                    ExitMyLock();
                    if (timeout.IsExpired)
                        return false;
                    spincount++;
                    Thread.SpinWait(spincount);
                    EnterMyLock();
                    //The per-thread structure may have been recycled as the lock is acquired (due to message pumping), load again.  
                    if (IsRwHashEntryChanged(lrwc))
                        lrwc = GetThreadRWCount(false);
                    continue;
                }

                // Drat, we need to wait.  Mark that we have waiters and wait.  
                if (readEvent == null) // Create the needed event  
                {
                    LazyCreateEvent(ref readEvent, false);
                    if (IsRwHashEntryChanged(lrwc))
                        lrwc = GetThreadRWCount(false);
                    continue; // since we left the lock, start over.  
                }

                retVal = WaitOnEvent(readEvent, ref numReadWaiters, timeout);
                if (!retVal) {
                    return false;
                }
                if (IsRwHashEntryChanged(lrwc))
                    lrwc = GetThreadRWCount(false);
            }

            ExitMyLock();
            return retVal;
        }

        private void EnterMyLock() {
            if (Interlocked.CompareExchange(ref myLock, 1, 0) != 0)
                EnterMyLockSpin();
        }

        private void EnterMyLockSpin() {
            int pc = Environment.ProcessorCount;   // 当前处理器的数量
            for (int i = 0; ; i++) {
                if (i < LockSpinCount && pc > 1) {
                    // 等待几十条指令，让另一个处理器释放锁
                    Thread.SpinWait(LockSpinCycles * (i + 1)); // Wait a few dozen instructions to let another processor release lock.  
                }
                else if (i < (LockSpinCount + LockSleep0Count)) {
                    Thread.Sleep(0); // Give up my quantum.  
                }
                else {
                    Thread.Sleep(1); // Give up my quantum.  
                }

                if (myLock == 0 && Interlocked.CompareExchange(ref myLock, 1, 0) == 0)
                    return;
            }
        }

        private void ExitMyLock() {
            Debug.Assert(myLock != 0, "Exiting spin lock that is not held");
            Volatile.Write(ref myLock, 0);
        }
        /// DontAllocate is set to true if the caller just wants to get an existing entry for this thread, but doesn't want to add one if an existing one could not be found.  
        private ReaderWriterCount GetThreadRWCount(bool dontAllocate) {
            ReaderWriterCount rwc = t_rwc;
            ReaderWriterCount empty = null;
            while (rwc != null) {
                if (rwc.lockID == this.lockID)
                    return rwc;

                if (!dontAllocate && empty == null && IsRWEntryEmpty(rwc))
                    empty = rwc;

                rwc = rwc.next;
            }

            if (dontAllocate)
                return null;

            if (empty == null) {
                empty = new ReaderWriterCount();
                empty.next = t_rwc;
                t_rwc = empty;
            }

            empty.lockID = this.lockID;
            return empty;
        }

        private static bool IsRWEntryEmpty(ReaderWriterCount rwc) {
            if (rwc.lockID == 0)
                return true;
            else if (rwc.readercount == 0 && rwc.writercount == 0 && rwc.upgradecount == 0)
                return true;
            else
                return false;
        }

        private void LazyCreateEvent(ref EventWaitHandle waitEvent, bool makeAutoResetEvent) {
            ExitMyLock();
            EventWaitHandle newEvent;
            if (makeAutoResetEvent)
                newEvent = new AutoResetEvent(false);
            else
                newEvent = new ManualResetEvent(false);
            EnterMyLock();
            if (waitEvent == null) // maybe someone snuck in.  
                waitEvent = newEvent;
            else
                newEvent.Close();
        }

        private bool WaitOnEvent(EventWaitHandle waitEvent, ref uint numWaiters, TimeoutTracker timeout) {
            waitEvent.Reset();
            numWaiters++;
            fNoWaiters = false;

            //Setting these bits will prevent new readers from getting in.  
            if (numWriteWaiters == 1)
                SetWritersWaiting();
            if (numWriteUpgradeWaiters == 1)
                SetUpgraderWaiting();

            bool waitSuccessful = false;
            ExitMyLock(); // Do the wait outside of any lock  

            try {
                waitSuccessful = waitEvent.WaitOne(timeout.RemainingMilliseconds);
            }
            finally {
                EnterMyLock();
                --numWaiters;

                if (numWriteWaiters == 0 && numWriteUpgradeWaiters == 0 && numUpgradeWaiters == 0 && numReadWaiters == 0)
                    fNoWaiters = true;

                if (numWriteWaiters == 0)
                    ClearWritersWaiting();
                if (numWriteUpgradeWaiters == 0)
                    ClearUpgraderWaiting();

                if (!waitSuccessful) // We may also be about to throw for some reason.  Exit myLock.  
                    ExitMyLock();
            }
            return waitSuccessful;
        }

        private bool IsRwHashEntryChanged(ReaderWriterCount lrwc) {
            return lrwc.lockID != this.lockID;
        }

        private bool IsWriterAcquired() {
            return (owners & ~WAITING_WRITERS) == 0;
        }

        private void SetWriterAcquired() {
            owners |= WRITER_HELD; // indicate we have a writer.
        }

        private void ClearWriterAcquired() {
            owners &= ~WRITER_HELD;
        }

        private void SetWritersWaiting() {
            owners |= WAITING_WRITERS;
        }

        private void ClearWritersWaiting() {
            owners &= ~WAITING_WRITERS;
        }

        private void SetUpgraderWaiting() {
            owners |= WAITING_UPGRADER;
        }

        private void ClearUpgraderWaiting() {
            owners &= ~WAITING_UPGRADER;
        }

        private uint GetNumReaders() {
            return owners & READER_MASK;
        }


        public void Dispose() {
            throw new NotImplementedException();
        }
    }
}
