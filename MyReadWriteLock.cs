using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace MyReadWriteLock {

    enum ThreadLockStatus { UNLOCK, READING, WRITING };

    internal class ThreadLockNode {
        public long lock_id;
        public ThreadLockStatus status;
        public ThreadLockNode next;
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

    class MyReadWriteLock {

        int inter_span_lock;

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

        // Every lock instance has a unique ID, which is used by ThreadLockNode to associate itself with the lock  
        // without holding a reference to it.  
        // 每个锁实例都有一个唯一的ID，ReaderWriterCount使用该ID将自身与锁相关联，而不保留对其的引用。
        static long s_nextLockID;
        long lockID;

        // See comments on ThreadLockNode.  
        [ThreadStatic]
        static ThreadLockNode t_rwc;

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


        /****************** read lock **********************/
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

            ThreadLockNode thread_lock_node = null;
            int id = Thread.CurrentThread.ManagedThreadId;

            if (id == writeLockOwnerId) {
                //Check for AW->AR  
                throw new Exception("Not allow read after write");
            }
            EnterMyLock();
            thread_lock_node = GetThreadRWCount(false);

            //Check if the reader lock is already acquired. Note, we could check the presence of a reader by not allocating rwc (But that would lead to two lookups in the common case. It's better to keep a count in the struucture).  
            if (thread_lock_node.status == ThreadLockStatus.READING) {
                ExitMyLock();
                throw new Exception("Not allow recursive read");
            }
            else if (id == upgradeLockOwnerId) {
                //The upgrade lock is already held.Update the global read counts and exit.  
                // 降级，可以直接拿到锁
                thread_lock_node.status = ThreadLockStatus.READING;
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
                    thread_lock_node.status = ThreadLockStatus.READING;
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
                    if (IsRwHashEntryChanged(thread_lock_node))
                        thread_lock_node = GetThreadRWCount(false);
                    continue;
                }

                // Drat, we need to wait.  Mark that we have waiters and wait.  
                if (readEvent == null) // Create the needed event  
                {
                    LazyCreateEvent(ref readEvent, false);
                    if (IsRwHashEntryChanged(thread_lock_node))
                        thread_lock_node = GetThreadRWCount(false);
                    continue; // since we left the lock, start over.  
                }

                retVal = WaitOnEvent(readEvent, ref numReadWaiters, timeout);
                if (!retVal) {
                    return false;
                }
                if (IsRwHashEntryChanged(thread_lock_node))
                    thread_lock_node = GetThreadRWCount(false);
            }

            ExitMyLock();
            return retVal;
        }

        public void ExitReadLock() {
            ThreadLockNode lrwc = null;
            EnterMyLock();
            lrwc = GetThreadRWCount(true);
            if (lrwc == null || lrwc.status != ThreadLockStatus.READING) {
                ExitMyLock();
                throw new Exception("mis match read");
            }

            --owners;
            lrwc.status = ThreadLockStatus.UNLOCK;
            ExitAndWakeUpAppropriateWaiters();    // 唤醒合适的waiters
        }

        /****************** write lock **********************/
        public void EnterWriteLock() {
            TryEnterWriteLock(-1);
        }
        public bool TryEnterWriteLock(int millisecondsTimeout) {
            return TryEnterWriteLock(new TimeoutTracker(millisecondsTimeout));
        }

        private bool TryEnterWriteLock(TimeoutTracker timeout) {
            bool result = false;
            try {
                result = TryEnterWriteLockCore(timeout);
            }
            finally { }
            return result;
        }

        private bool TryEnterWriteLockCore(TimeoutTracker timeout) {

            int id = Thread.CurrentThread.ManagedThreadId;
            ThreadLockNode lrwc;
            bool upgradingToWrite = false;

            if (id == writeLockOwnerId) {      //  不允许循环重入写
                //Check for AW->AW  
                throw new Exception("Not allow recursive write");
            }
            else if (id == upgradeLockOwnerId) {     // 允许可升级读锁升级为写锁
                //AU->AW case is allowed once.  
                upgradingToWrite = true;
            }

            EnterMyLock();
            lrwc = GetThreadRWCount(true);

            //Can't acquire write lock with reader lock held.  
            if (lrwc != null && lrwc.status == ThreadLockStatus.READING) {
                ExitMyLock();
                throw new Exception("Not allow write after read");
            }

            int spincount = 0;
            bool retVal = true;
            for (; ; ) {
                if (IsWriterAcquired()) {
                    // Good case, there is no contention, we are basically done  
                    SetWriterAcquired();
                    break;
                }

                //Check if there is just one upgrader, and no readers.Assumption: Only one thread can have the upgrade lock, so the following check will fail for all other threads that may sneak in when the upgrading thread is waiting.  

                if (upgradingToWrite) {
                    uint readercount = GetNumReaders();
                    if (readercount == 1) {
                        //Good case again, there is just one upgrader, and no readers.  
                        SetWriterAcquired(); // indicate we have a writer.  
                        break;
                    }
                    else if (readercount == 2) {
                        if (lrwc != null) {
                            if (IsRwHashEntryChanged(lrwc))
                                lrwc = GetThreadRWCount(false);

                            if (lrwc.status == ThreadLockStatus.READING) {
                                //Good case again, there is just one upgrader, and no readers.  
                                SetWriterAcquired(); // indicate we have a writer.  
                                break;
                            }
                        }
                    }
                }

                if (spincount < MaxSpinCount) {   // 在timeout内竞争等待
                    ExitMyLock();
                    if (timeout.IsExpired)
                        return false;
                    spincount++;
                    Thread.SpinWait(spincount);
                    EnterMyLock();
                    continue;
                }

                if (upgradingToWrite) {           // 可升级读升级为写，等待
                    if (waitUpgradeEvent == null) // Create the needed event  
                    {
                        LazyCreateEvent(ref waitUpgradeEvent, true);
                        continue; // since we left the lock, start over.  
                    }
                    retVal = WaitOnEvent(waitUpgradeEvent, ref numWriteUpgradeWaiters, timeout);

                    //The lock is not held in case of failure.  
                    if (!retVal)
                        return false;
                }
                else {                           // 写者等待
                    // Drat, we need to wait.  Mark that we have waiters and wait.  
                    if (writeEvent == null) // create the needed event.  
                    {
                        LazyCreateEvent(ref writeEvent, true);
                        continue; // since we left the lock, start over.  
                    }

                    retVal = WaitOnEvent(writeEvent, ref numWriteWaiters, timeout);
                    //The lock is not held in case of failure.  
                    if (!retVal)
                        return false;
                }
            }

            ExitMyLock();
            writeLockOwnerId = id;
            return true;
        }

        public void ExitWriteLock() {
#if DEBUG
            //Console.WriteLine("Debug: ExitWriteLock");
#endif
            ThreadLockNode lrwc;

            EnterMyLock();
            lrwc = GetThreadRWCount(false);

            if (lrwc == null) {
                ExitMyLock();
                throw new Exception("Mis match write");
            }


            //if (thread_lock_node.writercount < 1) {       // for recursive write
            //    ExitMyLock();
            //    throw new Exception("Mis match write");
            //}

            lrwc.status = ThreadLockStatus.UNLOCK;

            if (lrwc.status == ThreadLockStatus.WRITING) {
                ExitMyLock();
                return;
            }

            ClearWriterAcquired();

            writeLockOwnerId = -1;

            ExitAndWakeUpAppropriateWaiters();  // 唤醒
        }

        /****************** upgradeable read lock **********************/
        public void EnterUpgradeableReadLock() {
            TryEnterUpgradeableReadLock(-1);
        }

        public bool TryEnterUpgradeableReadLock(int millisecondsTimeout) {
            return TryEnterUpgradeableReadLock(new TimeoutTracker(millisecondsTimeout));
        }

        private bool TryEnterUpgradeableReadLock(TimeoutTracker timeout) {
            bool result = false;
            try {
                result = TryEnterUpgradeableReadLockCore(timeout);
            }
            finally { }
            return result;
        }

        private bool TryEnterUpgradeableReadLockCore(TimeoutTracker timeout) {

            int id = Thread.CurrentThread.ManagedThreadId;
            ThreadLockNode lrwc;

            if (id == upgradeLockOwnerId) {
                //Check for AU->AU  
                throw new Exception("Not allow recursive upgrade");
            }
            else if (id == writeLockOwnerId) {
                //Check for AU->AW  
                throw new Exception("Not allow upgrade after write");
            }

            EnterMyLock();
            lrwc = GetThreadRWCount(true);
            //Can't acquire upgrade lock with reader lock held.  
            if (lrwc != null && lrwc.status == ThreadLockStatus.READING) {
                ExitMyLock();
                throw new Exception("Not allow upgrade after read");
            }

            bool retVal = true;
            int spincount = 0;

            for (; ; ) {
                //Once an upgrade lock is taken, it's like having a reader lock held  
                //until upgrade or downgrade operations are performed.  

                if ((upgradeLockOwnerId == -1) && (owners < MAX_READER)) {
                    owners++;
                    upgradeLockOwnerId = id;
                    break;
                }

                if (spincount < MaxSpinCount) {
                    ExitMyLock();
                    if (timeout.IsExpired)
                        return false;
                    spincount++;
                    Thread.SpinWait(spincount);
                    EnterMyLock();
                    continue;
                }

                // Drat, we need to wait.  Mark that we have waiters and wait.  
                if (upgradeEvent == null) // Create the needed event  
                {
                    LazyCreateEvent(ref upgradeEvent, true);
                    continue; // since we left the lock, start over.  
                }

                //Only one thread with the upgrade lock held can proceed.  
                retVal = WaitOnEvent(upgradeEvent, ref numUpgradeWaiters, timeout);
                if (!retVal)
                    return false;
            }

            ExitMyLock();
            return true;
        }

        public void ExitUpgradeableReadLock() {
            ThreadLockNode lrwc;

            if (Thread.CurrentThread.ManagedThreadId != upgradeLockOwnerId) {
                //You have to be holding the upgrade lock to make this call.  
                throw new Exception("Mis match upgrade");
            }
            EnterMyLock();

            owners--;
            upgradeLockOwnerId = -1;

            ExitAndWakeUpAppropriateWaiters();   // 唤醒
        }



        /****************** wake up **********************/
        /// <summary>
        /// 唤醒
        /// </summary>
        private void ExitAndWakeUpAppropriateWaiters() {
            if (fNoWaiters) {
                ExitMyLock();
                return;
            }
            ExitAndWakeUpAppropriateWaitersPreferringWriters();   // 唤醒，优先唤醒写者
        }

        /// <summary>
        /// 唤醒，优先唤醒写者
        /// </summary>
        private void ExitAndWakeUpAppropriateWaitersPreferringWriters() {
            bool setUpgradeEvent = false;
            bool setReadEvent = false;
            uint readercount = GetNumReaders();

            if (readercount == 1 && numWriteUpgradeWaiters > 0) {  // 只有一个读者正在读，且这个读者待升级，则让这个读者升级
                //We have to be careful now, as we are droppping the lock. No new writes should be allowed to sneak in if an upgrade was pending.  
                ExitMyLock(); // Exit before signaling to improve efficiency (wakee will need the lock)  
                waitUpgradeEvent.Set(); // release all upgraders (however there can be at most one).  
            }
            else if (readercount == 0 && numWriteWaiters > 0) {    // 没有读者在读了，但有等待的写者，则释放一个写者
                ExitMyLock(); // Exit before signaling to improve efficiency (wakee will need the lock)  
                writeEvent.Set(); // release one writer.  
            }
            else if (readercount >= 0) {                           // 正在读的读者 >= 0
                if (numReadWaiters != 0 || numUpgradeWaiters != 0) {    // 有等待的普通读者，或有等待的可升级读者
                    if (numReadWaiters != 0)                            // 有等待的普通读者，释放读者
                        setReadEvent = true;

                    if (numUpgradeWaiters != 0 && upgradeLockOwnerId == -1) {   // 如果有等待的可升级读者，且暂时没有可升级读者在读
                        setUpgradeEvent = true;
                    }

                    ExitMyLock(); // Exit before signaling to improve efficiency (wakee will need the lock)  

                    if (setReadEvent)
                        readEvent.Set(); // release all readers.    // 释放所有普通读者

                    if (setUpgradeEvent)
                        upgradeEvent.Set(); //release one upgrader.   // 释放一个可升级读者
                }
                else
                    ExitMyLock();
            }
            else
                ExitMyLock();
        }

        private void EnterMyLock() {
            if (Interlocked.CompareExchange(ref inter_span_lock, 1, 0) != 0)
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

                if (inter_span_lock == 0 && Interlocked.CompareExchange(ref inter_span_lock, 1, 0) == 0)
                    return;
            }
        }

        private void ExitMyLock() {
            Debug.Assert(inter_span_lock != 0, "Exiting spin lock that is not held");
            Volatile.Write(ref inter_span_lock, 0);
        }

        /// DontAllocate is set to true if the caller just wants to get an existing entry for this thread, but doesn't want to add one if an existing one could not be found.  
        private ThreadLockNode GetThreadRWCount(bool dontAllocate) {
            ThreadLockNode rwc = t_rwc;
            ThreadLockNode empty = null;
            while (rwc != null) {
                if (rwc.lock_id == this.lockID)
                    return rwc;

                if (!dontAllocate && empty == null && IsRWEntryEmpty(rwc))
                    empty = rwc;

                rwc = rwc.next;
            }

            if (dontAllocate)
                return null;

            if (empty == null) {
                empty = new ThreadLockNode();
                empty.next = t_rwc;
                t_rwc = empty;
            }

            empty.lock_id = this.lockID;
            return empty;
        }

        private static bool IsRWEntryEmpty(ThreadLockNode rwc) {
            if (rwc.lock_id == 0)
                return true;
            else if (rwc.status == ThreadLockStatus.UNLOCK)
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

                if (!waitSuccessful) // We may also be about to throw for some reason.  Exit inter_span_lock.  
                    ExitMyLock();
            }
            return waitSuccessful;
        }

        private bool IsRwHashEntryChanged(ThreadLockNode lrwc) {
            return lrwc.lock_id != this.lockID;
        }

        /// <summary>
        /// 是否允许获取写锁
        /// 当该锁没有被任何进程获取时返回true
        /// </summary>
        /// <returns></returns>
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
    }
}
