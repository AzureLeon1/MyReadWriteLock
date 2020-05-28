using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace MyReadWriteLock {
    class Program {
        static void Main(string[] args) {

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

            // 3个写任务，共需要获取写锁9+11+17=37次
            tasks.Add(Task.Run(() => {
                
                for (int ctr = 1; ctr <= dot_net_knowledge.Length; ctr++)
                    my_cache.Add(ctr, dot_net_knowledge[ctr - 1]);

                // temp_cnt_items = dot_net_knowledge.Length;
                cnt_items = dot_net_knowledge.Length;
                Console.WriteLine("写者 - Task id {0} - 向缓存写入 {1} 项",
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
                        if (items!=0)
                            Console.WriteLine("读者 - Task id {0} - 读取 {1} 项: {2}",
                                          Task.CurrentId, items, output);
                    } while (items < cnt_items | cnt_items == 0);
                }));
            }
            // 一个可升级读任务，如果读到"Entity Framework"，则将其修改为"EF"
            tasks.Add(Task.Run(() => {
                Thread.Sleep(100);
                for (int ctr = 1; ctr <= my_cache.Count; ctr++) {
                    String value = my_cache.Read(ctr);
                    if (value == "Entity Framework")
                        if (my_cache.AddOrUpdate(ctr, "EF") != MyCache.AddOrUpdateStatus.Unchanged)
                            Console.WriteLine("Changed 'Entity Framework' to 'EF'");
                }
            }));

            // Wait for all 7 tasks to complete.
            Task.WaitAll(tasks.ToArray());

            // Display the final contents of the cache.
            Console.WriteLine();
            Console.WriteLine("缓存数据: ");
            for (int ctr = 1; ctr <= my_cache.Count; ctr++)
                Console.WriteLine("  第 {0} 项: {1}", ctr, my_cache.Read(ctr));
        }
    }
}
