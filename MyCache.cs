using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace MyReadWriteLock {

    /// <summary>
    /// 使用读写锁实现的一个共享缓存
    /// </summary>
    public class MyCache {
        // private ReaderWriterLockSlim cacheLock = new ReaderWriterLockSlim();
        private MyReadWriteLock cacheLock = new MyReadWriteLock();

        /// <summary>
        /// 共享资源对象：一个字典
        /// </summary>
        private Dictionary<int, string> innerCache = new Dictionary<int, string>();

        public int Count { get { return innerCache.Count; } }

        public string Read(int key) {
            cacheLock.EnterReadLock();
            Console.WriteLine("* LockInfo: 请求读锁成功！");
            try {
                return innerCache[key];
            }
            finally {
                Console.WriteLine("* LockInfo: 退出读锁！");
                cacheLock.ExitReadLock();
            }
        }

        public void Add(int key, string value) {
            cacheLock.EnterWriteLock();
            Console.WriteLine("* LockInfo: 请求写锁成功！");
            try {
                innerCache.Add(key, value);
            }
            finally {
                Console.WriteLine("* LockInfo: 退出写锁！");
                cacheLock.ExitWriteLock();
            }
        }

        public bool AddWithTimeout(int key, string value, int timeout) {
            if (cacheLock.TryEnterWriteLock(timeout)) {
                try {
                    innerCache.Add(key, value);
                }
                finally {
                    cacheLock.ExitWriteLock();
                }
                return true;
            }
            else {
                return false;
            }
        }

        public AddOrUpdateStatus AddOrUpdate(int key, string value) {
            cacheLock.EnterUpgradeableReadLock();
            Console.WriteLine("* LockInfo: 请求可升级读锁成功！");
            try {
                string result = null;
                if (innerCache.TryGetValue(key, out result)) {
                    if (result == value) {
                        return AddOrUpdateStatus.Unchanged;
                    }
                    else {
                        cacheLock.EnterWriteLock();
                        Console.WriteLine("* LockInfo: 请求写锁成功！");
                        try {
                            innerCache[key] = value;
                        }
                        finally {
                            Console.WriteLine("* LockInfo: 退出写锁！");
                            cacheLock.ExitWriteLock();
                        }
                        return AddOrUpdateStatus.Updated;
                    }
                }
                else {
                    cacheLock.EnterWriteLock();
                    Console.WriteLine("* LockInfo: 请求写锁成功！");
                    try {
                        innerCache.Add(key, value);
                    }
                    finally {
                        Console.WriteLine("* LockInfo: 退出写锁！");
                        cacheLock.ExitWriteLock();
                    }
                    return AddOrUpdateStatus.Added;
                }
            }
            finally {
                Console.WriteLine("* LockInfo: 退出可升级读锁！");
                cacheLock.ExitUpgradeableReadLock();
            }
        }

        public void Delete(int key) {
            cacheLock.EnterWriteLock();
            Console.WriteLine("* LockInfo: 请求写锁成功！");
            try {
                innerCache.Remove(key);
            }
            finally {
                Console.WriteLine("* LockInfo: 退出写锁！");
                cacheLock.ExitWriteLock();
            }
        }

        public enum AddOrUpdateStatus {
            Added,
            Updated,
            Unchanged
        };

        ~MyCache() {
            // if (cacheLock != null) cacheLock.Dispose();
        }
    }
}
