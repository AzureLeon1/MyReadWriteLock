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
        const int LockSpinCount = 10;    // 自旋次数
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
            lock_id = Interlocked.Increment(ref lock_id_incrementor);  // lock_id=0的节点是一个空的头节点，不对应实际锁
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
        /// <param name="timeout">超时时限</param>
        /// <returns>是否成功</returns>
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
        /// <param name="timeout">超时时限</param>
        /// <returns>是否成功</returns>
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
                    // 获取写锁
                    SetWriterAcquired();
                    break;
                }

                // 升级为写锁
                if (upgrading_to_write) {
                    uint readercount = GetNumReaders();
                    if (readercount == 1) {
                        // 只有当前线程，升级
                        SetWriterAcquired(); 
                        break;
                    }
                    else if (readercount == 2) {
                        // 只有当前线程（同时获得了读锁），也可以升级
                        if (thread_lock_node != null) {
                            if (IsRwHashEntryChanged(thread_lock_node))
                                thread_lock_node = GetThreadLockNode(false);

                            if (thread_lock_node.status == ThreadLockStatus.READING) {
                                SetWriterAcquired();  
                                break;
                            }
                        }
                    }
                }

                // 自旋等待
                if (spin_count < Maxspin_count) {  
                    ExitInterSpanLock();
                    if (timeout.IsExpired)
                        return false;
                    spin_count++;
                    Thread.SpinWait(spin_count);
                    EnterInterSpanLock();
                    continue;
                }

                // 创建 AutoResetEvent 等待升级
                if (upgrading_to_write) {     
                    if (waitUpgradeEvent == null) 
                    {
                        LazyCreateEvent(ref waitUpgradeEvent, true);
                        continue; 
                    }
                    return_value = WaitOnEvent(waitUpgradeEvent, ref numWriteUpgradeWaiters, timeout);
                    if (!return_value)
                        return false;
                }
                // 创建 AutoResetEvent 等待获取写锁
                else {                     
                    if (writeEvent == null) 
                    {
                        LazyCreateEvent(ref writeEvent, true);
                        continue; 
                    }
                    return_value = WaitOnEvent(writeEvent, ref numWriteWaiters, timeout);
                    if (!return_value)
                        return false;
                }
            }

            ExitInterSpanLock();
            write_lock_thread_id = id;
            return true;
        }

        /// <summary>
        /// 释放写锁
        /// </summary>
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
        /// <summary>
        /// 不设超时时限地请求获取可升级读锁
        /// </summary>
        public void EnterUpgradeableReadLock() {
            TryEnterUpgradeableReadLock(-1);
        }

        /// <summary>
        /// 尝试在规定超时时限内请求获取可升级读锁
        /// </summary>
        /// <param name="ms_timeout">超时时限</param>
        /// <returns>是否成功</returns>
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

        /// <summary>
        /// 请求可升级读锁核心逻辑
        /// </summary>
        /// <param name="timeout">超时时限</param>
        /// <returns>是否成功</returns>
        private bool TryEnterUpgradeableReadLockCore(TimeoutTracker timeout) {

            int id = Thread.CurrentThread.ManagedThreadId;
            ThreadLockNode thread_lock_node;

            if (id == upgrade_lock_thread_id) {
                // 已获取可升级读锁，不允许重入
                throw new Exception("Not allow recursive upgrade");
            }
            else if (id == write_lock_thread_id) {
                // 以获取写锁，不允许再获取可升级读锁
                throw new Exception("Not allow upgrade after write");
            }

            EnterInterSpanLock();
            thread_lock_node = GetThreadLockNode(true);
            if (thread_lock_node != null && thread_lock_node.status == ThreadLockStatus.READING) {
                // 以获取读锁，不允许再获取可升级读锁
                ExitInterSpanLock();
                throw new Exception("Not allow upgrade after read");
            }

            bool return_value = true;
            int spin_count = 0;

            for (; ; ) {

                // 可以获取
                if ((upgrade_lock_thread_id == -1) && (thread_count < MAX_READER)) {
                    thread_count++;
                    upgrade_lock_thread_id = id;
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
                    continue;
                }

                // 创建 AutoResetEvent 等待 
                if (upgradeEvent == null) 
                {
                    LazyCreateEvent(ref upgradeEvent, true);
                    continue; 
                }
                return_value = WaitOnEvent(upgradeEvent, ref numUpgradeWaiters, timeout);
                if (!return_value)
                    return false;
            }

            ExitInterSpanLock();
            return true;
        }

        /// <summary>
        /// 释放可升级读锁
        /// </summary>
        public void ExitUpgradeableReadLock() {

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
            if (flag_no_waiters) {    // 如果没有等待中的线程，直接释放内部轻量自旋锁，返回
                ExitInterSpanLock();
                return;
            }
            ExitAndWakeUpAppropriateWaitersPreferringWriters();   // 否则唤醒，优先唤醒写者
        }

        /// <summary>
        /// 唤醒，优先唤醒写者
        /// 唤醒顺序：
        /// 1 一个可升级读锁升级成写锁
        /// 2 一个写锁
        /// 3 所有读锁和一个新的可升级读锁
        /// </summary>
        private void ExitAndWakeUpAppropriateWaitersPreferringWriters() {
            bool setUpgradeEvent = false;
            bool setReadEvent = false;
            uint readercount = GetNumReaders();

            if (readercount == 1 && numWriteUpgradeWaiters > 0) {  // 只有一个读者正在读，且这个读者待升级，则让这个读者升级
                ExitInterSpanLock(); 
                waitUpgradeEvent.Set();  
            }
            else if (readercount == 0 && numWriteWaiters > 0) {    // 没有读者在读了，但有等待的写者，则释放一个写者
                ExitInterSpanLock(); 
                writeEvent.Set(); 
            }
            else if (readercount >= 0) {                           // 正在读的读者 >= 0
                if (numReadWaiters != 0 || numUpgradeWaiters != 0) {    // 有等待的普通读者，或有等待的可升级读者
                    if (numReadWaiters != 0)                            // 有等待的普通读者，释放读者
                        setReadEvent = true;

                    if (numUpgradeWaiters != 0 && upgrade_lock_thread_id == -1) {   // 如果有等待的可升级读者，且暂时没有可升级读者在读
                        setUpgradeEvent = true;
                    }

                    ExitInterSpanLock(); 

                    if (setReadEvent)
                        readEvent.Set();     // 释放所有普通读者

                    if (setUpgradeEvent)
                        upgradeEvent.Set();  // 释放一个可升级读者
                }
                else
                    ExitInterSpanLock();
            }
            else
                ExitInterSpanLock();
        }


        /****************** 内部轻量级自旋锁 **********************/
        /// <summary>
        /// 获取内部自旋锁
        /// </summary>
        private void EnterInterSpanLock() {
            if (Interlocked.CompareExchange(ref inter_span_lock, 1, 0) != 0)
                EnterInterLockSpin();
        }

        /// <summary>
        /// 如果不能马上获取内部自旋锁，则采取以下策略不断尝试获取：自旋多次 -> sleep(0)多次 -> sleep(1)多次
        /// </summary>
        private void EnterInterLockSpin() {
            int pc = Environment.ProcessorCount;   // 当前处理器的数量
            for (int i = 0; ; i++) {
                if (i < LockSpinCount && pc > 1) {
                    Thread.SpinWait(LockSpinCycles * (i + 1)); 
                }
                else if (i < (LockSpinCount + LockSleep0Count)) {
                    Thread.Sleep(0); 
                }
                else {
                    Thread.Sleep(1); 
                }

                if (inter_span_lock == 0 && Interlocked.CompareExchange(ref inter_span_lock, 1, 0) == 0)
                    return;
            }
        }

        /// <summary>
        /// 释放内部自旋锁
        /// </summary>
        private void ExitInterSpanLock() {
            Debug.Assert(inter_span_lock != 0, "Exiting spin lock that is not held");
            Volatile.Write(ref inter_span_lock, 0);
        }

        /****************** thread_lock_node 相关 **********************/
        /// <summary>
        /// 从该线程的ThreadLockNode链表中选出当前读写锁对应的那个节点
        /// </summary>
        /// <param name="dontAllocate">如果没有找到节点，是否不要创建</param>
        /// <returns>找到的节点</returns>
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

        /// <summary>
        /// thread_lock_node 是否仍对应当前读写锁
        /// </summary>
        /// <param name="thread_lock_node"></param>
        /// <returns></returns>
        private bool IsRwHashEntryChanged(ThreadLockNode thread_lock_node) {
            return thread_lock_node.lock_id != this.lock_id;
        }

        /// <summary>
        /// 当前线程是否还没有获取该锁
        /// </summary>
        /// <param name="thread_lock_node">线程读写锁节点thread_lock_node</param>
        /// <returns></returns>
        private static bool IsRWEntryEmpty(ThreadLockNode thread_lock_node) {
            if (thread_lock_node.lock_id == 0)   // 初始节点是无意义的空节点
                return true;
            else if (thread_lock_node.status == ThreadLockStatus.UNLOCK)  // 当前没有占有该锁（曾经占有过）
                return true;
            else
                return false;
        }

        /****************** 事件相关（创建事件、等待事件） **********************/
        /// <summary>
        /// 创建事件
        /// </summary>
        /// <param name="waitEvent">创建哪类事件</param>
        /// <param name="makeAutoResetEvent">是否是自动复位事件</param>
        private void LazyCreateEvent(ref EventWaitHandle waitEvent, bool makeAutoResetEvent) {
            ExitInterSpanLock();
            EventWaitHandle newEvent;
            if (makeAutoResetEvent)
                newEvent = new AutoResetEvent(false);
            else
                newEvent = new ManualResetEvent(false);
            EnterInterSpanLock();
            if (waitEvent == null)
                waitEvent = newEvent;
            else
                newEvent.Close();
        }

        /// <summary>
        /// 尝试再规定的时限内等待事件
        /// </summary>
        /// <param name="waitEvent">事件</param>
        /// <param name="numWaiters">事件类型计数</param>
        /// <param name="timeout">超时时限</param>
        /// <returns></returns>
        private bool WaitOnEvent(EventWaitHandle waitEvent, ref uint numWaiters, TimeoutTracker timeout) {
            waitEvent.Reset();
            numWaiters++;
            flag_no_waiters = false;

            if (numWriteWaiters == 1)
                SetWritersWaiting();
            if (numWriteUpgradeWaiters == 1)
                SetUpgraderWaiting();

            bool waitSuccessful = false;
            ExitInterSpanLock();

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

                if (!waitSuccessful) 
                    ExitInterSpanLock();
            }
            return waitSuccessful;
        }

        

        /********************** 对 thread_count 位运算相关 *****************************/
        /// <summary>
        /// 是否允许获取写锁
        /// 当该锁没有被任何进程获取时返回true
        /// </summary>
        /// <returns></returns>
        private bool IsWriterAcquired() {
            return (thread_count & ~WAITING_WRITERS) == 0;
        }

        private void SetWriterAcquired() {
            thread_count |= WRITER_HELD; 
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
