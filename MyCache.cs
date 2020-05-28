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
            try {
                return innerCache[key];
            }
            finally {
                cacheLock.ExitReadLock();
            }
        }

        public void Add(int key, string value) {
            cacheLock.EnterWriteLock();
            try {
                innerCache.Add(key, value);
            }
            finally {
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
            try {
                string result = null;
                if (innerCache.TryGetValue(key, out result)) {
                    if (result == value) {
                        return AddOrUpdateStatus.Unchanged;
                    }
                    else {
                        cacheLock.EnterWriteLock();
                        try {
                            innerCache[key] = value;
                        }
                        finally {
                            cacheLock.ExitWriteLock();
                        }
                        return AddOrUpdateStatus.Updated;
                    }
                }
                else {
                    cacheLock.EnterWriteLock();
                    try {
                        innerCache.Add(key, value);
                    }
                    finally {
                        cacheLock.ExitWriteLock();
                    }
                    return AddOrUpdateStatus.Added;
                }
            }
            finally {
                cacheLock.ExitUpgradeableReadLock();
            }
        }

        public void Delete(int key) {
            cacheLock.EnterWriteLock();
            try {
                innerCache.Remove(key);
            }
            finally {
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
