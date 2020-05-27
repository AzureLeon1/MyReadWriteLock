using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace MyReadWriteLock {

    /// <summary>
    /// 某个线程对于某个读写锁的状态：未获取该锁、以获取读锁、以获取写锁
    /// </summary>
    public enum ThreadLockStatus { UNLOCK, READING, WRITING };

    /// <summary>
    /// 线程相关的读写锁链表，链表中的每个节点对应一个读写锁
    /// </summary>
    internal class ThreadLockNode {
        public long lock_id;
        public ThreadLockStatus status;
        public ThreadLockNode next;
    }

    /// <summary>
    /// 实现请求读写锁的超时行为
    /// </summary>
    internal class TimeoutTracker {
        private int ms_total;
        private int ms_start;

        public TimeoutTracker(int ms_timeout) {
            if (ms_timeout < -1)
                throw new ArgumentOutOfRangeException("ms_timeout");
            ms_total = ms_timeout;
            if (ms_total != -1 && ms_total != 0)
                ms_start = Environment.TickCount;
            else
                ms_start = 0;
        }

        public int RemainingMilliseconds {
            get {
                if (ms_total == -1 || ms_total == 0)
                    return ms_total;

                int elapsed = Environment.TickCount - ms_start;
                // elapsed may be negative if TickCount has overflowed by 2^31 milliseconds.  
                if (elapsed < 0 || elapsed >= ms_total)
                    return 0;
                return ms_total - elapsed;
            }
        }

        public bool IsExpired {
            get {
                return RemainingMilliseconds == 0;
            }
        }
    }

    /// <summary>
    /// 读写锁
    /// </summary>
    class MyReadWriteLock {

        /****************** 内部轻量级自旋锁 **************************/
        // 自旋锁信号量，1表示锁定，0表示解锁，利用互锁函数组进行操作
        int inter_span_lock;

        // 自旋行为
        const int Lockspin_count = 10;    // 自旋次数
        const int LockSpinCycles = 20;   // 自旋圈数
        const int LockSleep0Count = 5;   // sleep(0)次数
        /************************************************************/

        /************ 控制对锁处于自旋等待状态的自旋行为 ***********/
        private const int Maxspin_count = 20;
        /********************************************************/


        /*********** 维护对锁处于 Event 等待状态的线程（不包括自旋等待状态的线程） ***********/
        // 线程进行 Event 等待需要的事件，readEvent是 ManualResetEvent，同时唤醒所有读者，其他的是AutoResetEvent，每次只唤醒一个等待者
        EventWaitHandle writeEvent;        // 线程等待获取写锁
        EventWaitHandle readEvent;         // 线程等待获取读锁（同时唤醒所有读者）
        EventWaitHandle upgradeEvent;      // 线程等待获取可升级读锁
        EventWaitHandle waitUpgradeEvent;  // 线程等待从可升级读状态下获取写锁

        // 各类等待线程的数量（只包括事件等待的线程，即设置了Event的线程，不包括自旋等待的线程）
        uint numWriteWaiters;        // 等待中的写者数  
        uint numReadWaiters;         // 等待中的读者数
        uint numWriteUpgradeWaiters; // 等待升级的可升级读者数（最大是1）   等待升级
        uint numUpgradeWaiters;      // 等待中的可升级读者数              等待开始读取

        // 方便快速查询，当上面4个num都是0时，该flag值为true
        bool flag_no_waiters;
        /************************************************************/


        /****************** 每个线程需要维护的读写锁链表 **************************/
        // 每个线程拥有一个链表，链表记录了该线程相关的读写锁，每个节点对应一个读写锁
        [ThreadStatic]
        static ThreadLockNode thread_tln;

        long lock_id;   // 在 ThreadLockNode 中记录读写锁的id，从而避免 ThreadLockNode 保存每个读写锁的引用
        static long lock_id_incrementor;  // lock_id 自动递增器，每次创建一个读写锁实例，用互锁函数族进行+1操作
        /**********************************************************************/


        /***************** 锁的共享变量 **********************/
        // 写锁、可升级读锁的所属线程的id，如果没有则为-1
        int upgrade_lock_thread_id;
        int write_lock_thread_id;

        // 32位分成四部分： 1+1+1+29 （详见文档）
        uint thread_count;

        // 操作thread_count需要的掩码
        private const uint WRITER_HELD = 0x80000000;
        private const uint WAITING_WRITERS = 0x40000000;
        private const uint WAITING_UPGRADER = 0x20000000;

        // 读者数量上限，-2是为了运算时防溢出
        private const uint MAX_READER = 0x10000000 - 2;
        private const uint READER_MASK = 0x10000000 - 1;
        /*****************************************************/

        /// <summary>
        /// 构造函数
        /// </summary>
        public MyReadWriteLock() {
            InitializeThreadCounts();
            flag_no_waiters = true;
            lock_id = Interlocked.Increment(ref lock_id_incrementor);
        }

        /// <summary>
        /// 初始化锁的线程计数
        /// </summary>
        private void InitializeThreadCounts() {
            upgrade_lock_thread_id = -1;
            write_lock_thread_id = -1;
        }


        /****************** read lock **********************/
        /// <summary>
        /// 不设超时时限地请求获取读锁
        /// </summary>
        public void EnterReadLock() {
            TryEnterReadLock(-1);
        }

        /// <summary>
        /// 尝试在规定超时时限内请求获取读锁
        /// </summary>
        /// <param name="ms_timeout">超时时限</param>
        /// <returns>是否成功</returns>
        public bool TryEnterReadLock(int ms_timeout) {
            return TryEnterReadLock(new TimeoutTracker(ms_timeout));
        }

        private bool TryEnterReadLock(TimeoutTracker timeout) {
            bool result = false;
            try {
                result = TryEnterReadLockCore(timeout);
            }
            finally { }
            return result;
        }

        /// <summary>
        /// 请求读锁核心逻辑
        /// </summary>
        /// <param name="timeout"></param>
        /// <returns></returns>
        private bool TryEnterReadLockCore(TimeoutTracker timeout) {

            ThreadLockNode thread_lock_node = null;
            int id = Thread.CurrentThread.ManagedThreadId;

            if (id == write_lock_thread_id) {
                // 不允许读锁直接降级为写锁
                throw new Exception("Not allow read after write");
            }
            EnterInterSpanLock();
            thread_lock_node = GetThreadLockNode(false);

            if (thread_lock_node.status == ThreadLockStatus.READING) {
                // 已有读锁，不允许重入
                ExitInterSpanLock();
                throw new Exception("Not allow recursive read");
            }
            else if (id == upgrade_lock_thread_id) {
                // 以获取可升级读锁的线程请求普通读锁，可以直接拿到锁（不会释放原来的可升级读锁）
                thread_lock_node.status = ThreadLockStatus.READING;
                thread_count++;
                ExitInterSpanLock();
                return true;
            }

            bool return_value = true;
            int spin_count = 0;

            for (; ; ) {
                // 正常获取读锁
                if (thread_count < MAX_READER) {
                    thread_count++;
                    thread_lock_node.status = ThreadLockStatus.READING;
                    break;
                }

                // 自旋等待
                if (spin_count < Maxspin_count) {
                    ExitInterSpanLock();
                    if (timeout.IsExpired)
                        return false;
                    spin_count++;
                    Thread.SpinWait(spin_count);
                    EnterInterSpanLock();
                    if (IsRwHashEntryChanged(thread_lock_node))
                        thread_lock_node = GetThreadLockNode(false);
                    continue;
                }

                // 设置 ManualResetEvent 等待 
                if (readEvent == null)  
                {
                    LazyCreateEvent(ref readEvent, false);
                    if (IsRwHashEntryChanged(thread_lock_node))
                        thread_lock_node = GetThreadLockNode(false);
                    continue; 
                }
                return_value = WaitOnEvent(readEvent, ref numReadWaiters, timeout);
                if (!return_value) {
                    return false;
                }
                if (IsRwHashEntryChanged(thread_lock_node))
                    thread_lock_node = GetThreadLockNode(false);
            }

            ExitInterSpanLock();
            return return_value;
        }

        /// <summary>
        /// 释放读锁
        /// </summary>
        public void ExitReadLock() {
            ThreadLockNode thread_lock_node = null;
            EnterInterSpanLock();
            thread_lock_node = GetThreadLockNode(true);
            if (thread_lock_node == null || thread_lock_node.status != ThreadLockStatus.READING) {
                ExitInterSpanLock();
                throw new Exception("mis match read");
            }

            --thread_count;
            thread_lock_node.status = ThreadLockStatus.UNLOCK;
            ExitAndWakeUpAppropriateWaiters();    // 唤醒合适的waiters
        }

        /****************** write lock **********************/
        /// <summary>
        /// 不设超时时限地请求获取写锁
        /// </summary>
        public void EnterWriteLock() {
            TryEnterWriteLock(-1);
        }

        /// <summary>
        /// 尝试在规定超时时限内请求获取写锁
        /// </summary>
        /// <param name="ms_timeout">超时时限</param>
        /// <returns>是否成功</returns>
        public bool TryEnterWriteLock(int ms_timeout) {
            return TryEnterWriteLock(new TimeoutTracker(ms_timeout));
        }

        private bool TryEnterWriteLock(TimeoutTracker timeout) {
            bool result = false;
            try {
                result = TryEnterWriteLockCore(timeout);
            }
            finally { }
            return result;
        }

        /// <summary>
        /// 请求写锁核心逻辑
        /// </summary>
        /// <param name="timeout"></param>
        /// <returns></returns>
        private bool TryEnterWriteLockCore(TimeoutTracker timeout) {

            int id = Thread.CurrentThread.ManagedThreadId;
            ThreadLockNode thread_lock_node;
            bool upgrading_to_write = false;

            if (id == write_lock_thread_id) {      
                // 以获取写锁，不允许重入
                throw new Exception("Not allow recursive write");
            }
            else if (id == upgrade_lock_thread_id) {     
                // 允许可升级读锁升级为写锁
                upgrading_to_write = true;
            }

            EnterInterSpanLock();
            thread_lock_node = GetThreadLockNode(true);

            if (thread_lock_node != null && thread_lock_node.status == ThreadLockStatus.READING) {
                // 不允许直接从读锁升级为写锁
                ExitInterSpanLock();
                throw new Exception("Not allow write after read");
            }

            int spin_count = 0;
            bool return_value = true;
            for (; ; ) {
                if (IsWriterAcquired()) {
                    // Good case, there is no contention, we are basically done  
                    SetWriterAcquired();
                    break;
                }

                //Check if there is just one upgrader, and no readers.Assumption: Only one thread can have the upgrade lock, so the following check will fail for all other threads that may sneak in when the upgrading thread is waiting.  

                if (upgrading_to_write) {
                    uint readercount = GetNumReaders();
                    if (readercount == 1) {
                        //Good case again, there is just one upgrader, and no readers.  
                        SetWriterAcquired(); // indicate we have a writer.  
                        break;
                    }
                    else if (readercount == 2) {
                        if (thread_lock_node != null) {
                            if (IsRwHashEntryChanged(thread_lock_node))
                                thread_lock_node = GetThreadLockNode(false);

                            if (thread_lock_node.status == ThreadLockStatus.READING) {
                                //Good case again, there is just one upgrader, and no readers.  
                                SetWriterAcquired(); // indicate we have a writer.  
                                break;
                            }
                        }
                    }
                }

                if (spin_count < Maxspin_count) {   // 在timeout内竞争等待
                    ExitInterSpanLock();
                    if (timeout.IsExpired)
                        return false;
                    spin_count++;
                    Thread.SpinWait(spin_count);
                    EnterInterSpanLock();
                    continue;
                }

                if (upgrading_to_write) {           // 可升级读升级为写，等待
                    if (waitUpgradeEvent == null) // Create the needed event  
                    {
                        LazyCreateEvent(ref waitUpgradeEvent, true);
                        continue; // since we left the lock, start over.  
                    }
                    return_value = WaitOnEvent(waitUpgradeEvent, ref numWriteUpgradeWaiters, timeout);

                    //The lock is not held in case of failure.  
                    if (!return_value)
                        return false;
                }
                else {                           // 写者等待
                    // Drat, we need to wait.  Mark that we have waiters and wait.  
                    if (writeEvent == null) // create the needed event.  
                    {
                        LazyCreateEvent(ref writeEvent, true);
                        continue; // since we left the lock, start over.  
                    }

                    return_value = WaitOnEvent(writeEvent, ref numWriteWaiters, timeout);
                    //The lock is not held in case of failure.  
                    if (!return_value)
                        return false;
                }
            }

            ExitInterSpanLock();
            write_lock_thread_id = id;
            return true;
        }

        public void ExitWriteLock() {
#if DEBUG
            //Console.WriteLine("Debug: ExitWriteLock");
#endif
            ThreadLockNode thread_lock_node;

            EnterInterSpanLock();
            thread_lock_node = GetThreadLockNode(false);

            if (thread_lock_node == null) {
                ExitInterSpanLock();
                throw new Exception("Mis match write");
            }

            thread_lock_node.status = ThreadLockStatus.UNLOCK;

            if (thread_lock_node.status == ThreadLockStatus.WRITING) {
                ExitInterSpanLock();
                return;
            }

            ClearWriterAcquired();

            write_lock_thread_id = -1;

            ExitAndWakeUpAppropriateWaiters();  // 唤醒
        }

        /****************** upgradeable read lock **********************/
        public void EnterUpgradeableReadLock() {
            TryEnterUpgradeableReadLock(-1);
        }

        public bool TryEnterUpgradeableReadLock(int ms_timeout) {
            return TryEnterUpgradeableReadLock(new TimeoutTracker(ms_timeout));
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
            ThreadLockNode thread_lock_node;

            if (id == upgrade_lock_thread_id) {
                //Check for AU->AU  
                throw new Exception("Not allow recursive upgrade");
            }
            else if (id == write_lock_thread_id) {
                //Check for AU->AW  
                throw new Exception("Not allow upgrade after write");
            }

            EnterInterSpanLock();
            thread_lock_node = GetThreadLockNode(true);
            //Can't acquire upgrade lock with reader lock held.  
            if (thread_lock_node != null && thread_lock_node.status == ThreadLockStatus.READING) {
                ExitInterSpanLock();
                throw new Exception("Not allow upgrade after read");
            }

            bool return_value = true;
            int spin_count = 0;

            for (; ; ) {
                //Once an upgrade lock is taken, it's like having a reader lock held  
                //until upgrade or downgrade operations are performed.  

                if ((upgrade_lock_thread_id == -1) && (thread_count < MAX_READER)) {
                    thread_count++;
                    upgrade_lock_thread_id = id;
                    break;
                }

                if (spin_count < Maxspin_count) {
                    ExitInterSpanLock();
                    if (timeout.IsExpired)
                        return false;
                    spin_count++;
                    Thread.SpinWait(spin_count);
                    EnterInterSpanLock();
                    continue;
                }

                // Drat, we need to wait.  Mark that we have waiters and wait.  
                if (upgradeEvent == null) // Create the needed event  
                {
                    LazyCreateEvent(ref upgradeEvent, true);
                    continue; // since we left the lock, start over.  
                }

                //Only one thread with the upgrade lock held can proceed.  
                return_value = WaitOnEvent(upgradeEvent, ref numUpgradeWaiters, timeout);
                if (!return_value)
                    return false;
            }

            ExitInterSpanLock();
            return true;
        }

        public void ExitUpgradeableReadLock() {
            ThreadLockNode thread_lock_node;

            if (Thread.CurrentThread.ManagedThreadId != upgrade_lock_thread_id) {
                //You have to be holding the upgrade lock to make this call.  
                throw new Exception("Mis match upgrade");
            }
            EnterInterSpanLock();

            thread_count--;
            upgrade_lock_thread_id = -1;

            ExitAndWakeUpAppropriateWaiters();   // 唤醒
        }



        /****************** wake up **********************/
        /// <summary>
        /// 唤醒
        /// </summary>
        private void ExitAndWakeUpAppropriateWaiters() {
            if (flag_no_waiters) {
                ExitInterSpanLock();
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
                ExitInterSpanLock(); // Exit before signaling to improve efficiency (wakee will need the lock)  
                waitUpgradeEvent.Set(); // release all upgraders (however there can be at most one).  
            }
            else if (readercount == 0 && numWriteWaiters > 0) {    // 没有读者在读了，但有等待的写者，则释放一个写者
                ExitInterSpanLock(); // Exit before signaling to improve efficiency (wakee will need the lock)  
                writeEvent.Set(); // release one writer.  
            }
            else if (readercount >= 0) {                           // 正在读的读者 >= 0
                if (numReadWaiters != 0 || numUpgradeWaiters != 0) {    // 有等待的普通读者，或有等待的可升级读者
                    if (numReadWaiters != 0)                            // 有等待的普通读者，释放读者
                        setReadEvent = true;

                    if (numUpgradeWaiters != 0 && upgrade_lock_thread_id == -1) {   // 如果有等待的可升级读者，且暂时没有可升级读者在读
                        setUpgradeEvent = true;
                    }

                    ExitInterSpanLock(); // Exit before signaling to improve efficiency (wakee will need the lock)  

                    if (setReadEvent)
                        readEvent.Set(); // release all readers.    // 释放所有普通读者

                    if (setUpgradeEvent)
                        upgradeEvent.Set(); //release one upgrader.   // 释放一个可升级读者
                }
                else
                    ExitInterSpanLock();
            }
            else
                ExitInterSpanLock();
        }

        private void EnterInterSpanLock() {
            if (Interlocked.CompareExchange(ref inter_span_lock, 1, 0) != 0)
                EnterMyLockSpin();
        }

        private void EnterMyLockSpin() {
            int pc = Environment.ProcessorCount;   // 当前处理器的数量
            for (int i = 0; ; i++) {
                if (i < Lockspin_count && pc > 1) {
                    // 等待几十条指令，让另一个处理器释放锁
                    Thread.SpinWait(LockSpinCycles * (i + 1)); // Wait a few dozen instructions to let another processor release lock.  
                }
                else if (i < (Lockspin_count + LockSleep0Count)) {
                    Thread.Sleep(0); // Give up my quantum.  
                }
                else {
                    Thread.Sleep(1); // Give up my quantum.  
                }

                if (inter_span_lock == 0 && Interlocked.CompareExchange(ref inter_span_lock, 1, 0) == 0)
                    return;
            }
        }

        private void ExitInterSpanLock() {
            Debug.Assert(inter_span_lock != 0, "Exiting spin lock that is not held");
            Volatile.Write(ref inter_span_lock, 0);
        }

        /// DontAllocate is set to true if the caller just wants to get an existing entry for this thread, but doesn't want to add one if an existing one could not be found.  
        private ThreadLockNode GetThreadLockNode(bool dontAllocate) {
            ThreadLockNode rwc = thread_tln;
            ThreadLockNode empty = null;
            while (rwc != null) {
                if (rwc.lock_id == this.lock_id)
                    return rwc;

                if (!dontAllocate && empty == null && IsRWEntryEmpty(rwc))
                    empty = rwc;

                rwc = rwc.next;
            }

            if (dontAllocate)
                return null;

            if (empty == null) {
                empty = new ThreadLockNode();
                empty.next = thread_tln;
                thread_tln = empty;
            }

            empty.lock_id = this.lock_id;
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
            ExitInterSpanLock();
            EventWaitHandle newEvent;
            if (makeAutoResetEvent)
                newEvent = new AutoResetEvent(false);
            else
                newEvent = new ManualResetEvent(false);
            EnterInterSpanLock();
            if (waitEvent == null) // maybe someone snuck in.  
                waitEvent = newEvent;
            else
                newEvent.Close();
        }

        private bool WaitOnEvent(EventWaitHandle waitEvent, ref uint numWaiters, TimeoutTracker timeout) {
            waitEvent.Reset();
            numWaiters++;
            flag_no_waiters = false;

            //Setting these bits will prevent new readers from getting in.  
            if (numWriteWaiters == 1)
                SetWritersWaiting();
            if (numWriteUpgradeWaiters == 1)
                SetUpgraderWaiting();

            bool waitSuccessful = false;
            ExitInterSpanLock(); // Do the wait outside of any lock  

            try {
                waitSuccessful = waitEvent.WaitOne(timeout.RemainingMilliseconds);
            }
            finally {
                EnterInterSpanLock();
                --numWaiters;

                if (numWriteWaiters == 0 && numWriteUpgradeWaiters == 0 && numUpgradeWaiters == 0 && numReadWaiters == 0)
                    flag_no_waiters = true;

                if (numWriteWaiters == 0)
                    ClearWritersWaiting();
                if (numWriteUpgradeWaiters == 0)
                    ClearUpgraderWaiting();

                if (!waitSuccessful) // We may also be about to throw for some reason.  Exit inter_span_lock.  
                    ExitInterSpanLock();
            }
            return waitSuccessful;
        }

        private bool IsRwHashEntryChanged(ThreadLockNode thread_lock_node) {
            return thread_lock_node.lock_id != this.lock_id;
        }

        /// <summary>
        /// 是否允许获取写锁
        /// 当该锁没有被任何进程获取时返回true
        /// </summary>
        /// <returns></returns>
        private bool IsWriterAcquired() {
            return (thread_count & ~WAITING_WRITERS) == 0;
        }

        private void SetWriterAcquired() {
            thread_count |= WRITER_HELD; // indicate we have a writer.
        }

        private void ClearWriterAcquired() {
            thread_count &= ~WRITER_HELD;
        }

        private void SetWritersWaiting() {
            thread_count |= WAITING_WRITERS;
        }

        private void ClearWritersWaiting() {
            thread_count &= ~WAITING_WRITERS;
        }

        private void SetUpgraderWaiting() {
            thread_count |= WAITING_UPGRADER;
        }

        private void ClearUpgraderWaiting() {
            thread_count &= ~WAITING_UPGRADER;
        }

        private uint GetNumReaders() {
            return thread_count & READER_MASK;
        }
    }
}
