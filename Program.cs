using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace MyReadWriteLock {
    class Program {
        static void Main(string[] args) {
            // var sc = new MyCache();
            var sc = new MyCache2();
            var tasks = new List<Task>();
            int itemsWritten = 0;

            // Execute a writer.
            tasks.Add(Task.Run(() => {
                String[] vegetables = { "broccoli", "cauliflower",
                                                          "carrot", "sorrel", "baby turnip",
                                                          "beet", "brussel sprout",
                                                          "cabbage", "plantain",
                                                          "spinach", "grape leaves",
                                                          "lime leaves", "corn",
                                                          "radish", "cucumber",
                                                          "raddichio", "lima beans" };
                for (int ctr = 1; ctr <= vegetables.Length; ctr++)
                    sc.Add(ctr, vegetables[ctr - 1]);

                itemsWritten = vegetables.Length;
                Console.WriteLine("Task {0} wrote {1} items\n",
                                  Task.CurrentId, itemsWritten);
            }));
            // Execute two readers, one to read from first to last and the second from last to first.
            for (int ctr = 0; ctr <= 1; ctr++) {
                bool desc = Convert.ToBoolean(ctr);
                tasks.Add(Task.Run(() => {
                    int start, last, step;
                    int items;
                    do {
                        String output = String.Empty;
                        items = sc.Count;
                        if (!desc) {
                            start = 1;
                            step = 1;
                            last = items;
                        }
                        else {
                            start = items;
                            step = -1;
                            last = 1;
                        }

                        for (int index = start; desc ? index >= last : index <= last; index += step)
                            output += String.Format("[{0}] ", sc.Read(index));

                        Console.WriteLine("Task {0} read {1} items: {2}\n",
                                          Task.CurrentId, items, output);
                    } while (items < itemsWritten | itemsWritten == 0);
                }));
            }
            // Execute a red/update task.
            tasks.Add(Task.Run(() => {
                Thread.Sleep(100);
                for (int ctr = 1; ctr <= sc.Count; ctr++) {
                    String value = sc.Read(ctr);
                    if (value == "cucumber")
                        if (sc.AddOrUpdate(ctr, "green bean") != MyCache2.AddOrUpdateStatus.Unchanged)
                            Console.WriteLine("Changed 'cucumber' to 'green bean'");
                }
            }));

            // Wait for all three tasks to complete.
            Task.WaitAll(tasks.ToArray());

            // Display the final contents of the cache.
            Console.WriteLine();
            Console.WriteLine("Values in synchronized cache: ");
            for (int ctr = 1; ctr <= sc.Count; ctr++)
                Console.WriteLine("   {0}: {1}", ctr, sc.Read(ctr));
        }
    }
}
