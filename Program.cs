using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace MyReadWriteLock {
    class Program {
        static void Main(string[] args) {

            Test1();    // 测试读者并发、读写互斥、写者优先、读锁升级
            Test2();    // 测试写者排他
        }

        private static void Test1() {
            Console.WriteLine("########## Test 1 #########");
            var my_cache = new MyCache();   // 共享资源：缓存
            var tasks = new List<Task>();   // 共享资源访问者：线程

            // 待读者写入缓存的数据
            String[] dot_net_knowledge = { "C#", ".NET",
                                           "IL", "assembly", "value_type",
                                           "reference_type", "CLR",
                                           "COM", "C++/CLI",
                                           "private", "protected",
                                           "public", "internal", "abstract",
                                           "sealed", "static",
                                           "readonly", "virtual",
                                           "new", "override",
                                           "garbage_collection", "reference_counting",
                                           "interlocked", "Monitor", "Mutex",
                                           "Semaphore", "AutoResetEvent",
                                           "ManualResetEvent", "WaitAny",
                                           "WaitAll", "Task",
                                           "async", "await",
                                           "LINQ", "ADO.NET",
                                           "Entity Framework"};

            int cnt_items = 0;
            // int temp_cnt_items = 0;

            // 1个写任务，共需要获取写锁36次
            tasks.Add(Task.Run(() => {

                for (int ctr = 1; ctr <= dot_net_knowledge.Length; ctr++)
                    my_cache.Add(ctr, dot_net_knowledge[ctr - 1]);

                // temp_cnt_items = dot_net_knowledge.Length;
                cnt_items = dot_net_knowledge.Length;
                Console.WriteLine("# TaskInfo: 写者 - Task id {0} - 向缓存写入 {1} 项",
                                  Task.CurrentId, cnt_items);
            }));

            // 3个读任务，读取缓存中已有的全部数据。会获取读锁很多次，直到写任务写入完成，且三个读任务都读到了全部的缓存数据
            for (int ctr = 0; ctr <= 2; ctr++) {
                tasks.Add(Task.Run(() => {
                    int items;
                    do {
                        String output = String.Empty;

                        items = my_cache.Count;
                        for (int index = 1; index <= items; index++)
                            output += String.Format("[{0}] ", my_cache.Read(index));
                        if (items != 0)
                            Console.WriteLine("# TaskInfo: 读者 - Task id {0} - 读取 {1} 项: {2}",
                                          Task.CurrentId, items, output);
                    } while (items < cnt_items | cnt_items == 0);
                }));
            }
            // 一个可升级读任务，如果读到"Entity Framework"，则将其修改为"EF"
            tasks.Add(Task.Run(() => {
                Thread.Sleep(1000);
                for (int ctr = 1; ctr <= my_cache.Count; ctr++) {
                    String value = my_cache.Read(ctr);
                    if (value == "Entity Framework")
                        if (my_cache.AddOrUpdate(ctr, "EF") != MyCache.AddOrUpdateStatus.Unchanged)
                            Console.WriteLine("# TaskInfo: Changed 'Entity Framework' to 'EF'");
                }
            }));

            // Wait for all tasks to complete.
            Task.WaitAll(tasks.ToArray());
        }

        private static void Test2() {
            Console.WriteLine("########## Test 2 #########");
            var my_cache = new MyCache();   // 共享资源：缓存
            var tasks = new List<Task>();   // 共享资源访问者：线程

            // 待读者写入缓存的数据
            String[] dot_net_knowledge_1 = { "C#", ".NET",
                                           "IL", "assembly", "value_type",
                                           "reference_type", "CLR",
                                           "COM", "C++/CLI"};
            String[] dot_net_knowledge_2 = { 
                                           "private", "protected",
                                           "public", "internal", "abstract",
                                           "sealed", "static",
                                           "readonly", "virtual",
                                           "new", "override",
                                           "garbage_collection", "reference_counting"};
            String[] dot_net_knowledge_3 = {
                                           "interlocked", "Monitor", "Mutex",
                                           "Semaphore", "AutoResetEvent",
                                           "ManualResetEvent", "WaitAny",
                                           "WaitAll", "Task",
                                           "async", "await",
                                           "LINQ", "ADO.NET",
                                           "Entity Framework"};

            int cnt_items = 0;
            // int temp_cnt_items = 0;

            // 3个写任务，会出现并发的写锁请求
            tasks.Add(Task.Run(() => {

                for (int ctr = 1; ctr <= dot_net_knowledge_1.Length; ctr++)
                    my_cache.Add(ctr, dot_net_knowledge_1[ctr - 1]);

                // temp_cnt_items = dot_net_knowledge.Length;
                cnt_items = dot_net_knowledge_1.Length;
                Console.WriteLine("# TaskInfo: 写者 - Task id {0} - 向缓存写入 {1} 项",
                                  Task.CurrentId, cnt_items);
            }));
            tasks.Add(Task.Run(() => {

                for (int ctr = 1; ctr <= dot_net_knowledge_2.Length; ctr++)
                    my_cache.Add(dot_net_knowledge_1.Length + ctr, dot_net_knowledge_2[ctr - 1]);

                // temp_cnt_items = dot_net_knowledge.Length;
                cnt_items = dot_net_knowledge_2.Length;
                Console.WriteLine("# TaskInfo: 写者 - Task id {0} - 向缓存写入 {1} 项",
                                  Task.CurrentId, cnt_items);
            }));
            tasks.Add(Task.Run(() => {

                for (int ctr = 1; ctr <= dot_net_knowledge_3.Length; ctr++)
                    my_cache.Add(dot_net_knowledge_1.Length + dot_net_knowledge_2.Length + ctr, dot_net_knowledge_3[ctr - 1]);

                // temp_cnt_items = dot_net_knowledge.Length;
                cnt_items = dot_net_knowledge_3.Length;
                Console.WriteLine("# TaskInfo: 写者 - Task id {0} - 向缓存写入 {1} 项",
                                  Task.CurrentId, cnt_items);
            }));

            // Wait for all tasks to complete.
            Task.WaitAll(tasks.ToArray());
        }
    }

}
