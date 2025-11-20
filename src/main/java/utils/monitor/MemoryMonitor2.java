// // 文件：utils/monitor/MemoryMonitor.java
// package utils.monitor;

// import benchmark.synchronize.tasks.FreshnessExecutor;
// import org.apache.lucene.util.RamUsageEstimator;

// import java.io.BufferedWriter;
// import java.io.File;
// import java.io.FileWriter;
// import java.io.IOException;
// import java.io.UncheckedIOException;
// import java.text.SimpleDateFormat;
// import java.util.Date;
// import java.util.List;
// import java.util.Locale;
// import java.util.Map;
// import java.util.concurrent.*;
// import java.util.concurrent.atomic.AtomicBoolean;

// /**
//  * 定时监控 FreshnessExecutor 的四分片 TRACE_SHARDS 的内存占用（MB）。
//  * CSV 列：StartTime,FinishTime,TotalMB,Shard0_MB,Shard1_MB,Shard2_MB,Shard3_MB
//  */
// public class MemoryMonitor2 {

//     /* ───────────── 可调参数 ───────────── */
//     private static final long SAMPLE_INTERVAL_MS = 1000; // 采样间隔（毫秒）
//     private static final boolean USE_RAM_ESTIMATOR = true; // 使用 RamUsageEstimator 估算

//     /* ───────────── 成员字段 ───────────── */
//     private final File file;
//     private final BlockingQueue<String> queue = new LinkedBlockingQueue<>();
//     private final ScheduledExecutorService scheduler;
//     private final ExecutorService worker;
//     private final AtomicBoolean sampling = new AtomicBoolean(false);
//     private final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.ROOT);
//     private Thread writerThread;

//     /* ───────────── 构造 ───────────── */
//     public MemoryMonitor2(String resultDir) {
//         this.file = new File(resultDir, "trace_memory_shards.csv");

//         scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
//             Thread t = new Thread(r, "TraceMemMonitor-Scheduler");
//             t.setDaemon(true);
//             return t;
//         });

//         worker = Executors.newSingleThreadExecutor(r -> {
//             Thread t = new Thread(r, "TraceMemMonitor-Worker");
//             t.setDaemon(true);
//             return t;
//         });

//         initFile();
//     }

//     /* ───────────── Public API ───────────── */
//     public void monitor() {
//         startWriter();
//         startSampling();
//     }

//     public void stop() {
//         scheduler.shutdownNow();
//         worker.shutdownNow();
//         queue.offer("__POISON__");
//         if (writerThread != null) writerThread.interrupt();
//     }

//     /* ───────────── 内部逻辑 ───────────── */
//     private void initFile() {
//         try {
//             if (!file.exists()) {
//                 File parent = file.getParentFile();
//                 if (parent != null) parent.mkdirs();
//                 try (BufferedWriter h = new BufferedWriter(new FileWriter(file, true))) {
//                     h.write("StartTime,FinishTime,TotalMB,Shard0_MB,Shard1_MB,Shard2_MB,Shard3_MB");
//                     h.newLine();
//                 }
//             }
//         } catch (IOException e) {
//             throw new UncheckedIOException(e);
//         }
//     }

//     private void startWriter() {
//         writerThread = new Thread(() -> {
//             try (BufferedWriter out = new BufferedWriter(new FileWriter(file, true))) {
//                 while (true) {
//                     String line = queue.take();
//                     if ("__POISON__".equals(line)) break;
//                     out.write(line);
//                     out.newLine();
//                     out.flush();
//                 }
//             } catch (InterruptedException ignored) {
//             } catch (IOException ioe) {
//                 ioe.printStackTrace();
//             }
//         }, "TraceMemMonitor-Writer");
//         writerThread.setDaemon(true);
//         writerThread.start();
//     }

//     private void startSampling() {
//         scheduler.scheduleAtFixedRate(
//             () -> {
//                 if (!sampling.compareAndSet(false, true)) return; // 防重入堆积
//                 worker.submit(() -> {
//                     try {
//                         sampleOnce();
//                     } catch (Exception e) {
//                         e.printStackTrace();
//                     } finally {
//                         sampling.set(false);
//                     }
//                 });
//             },
//             0, SAMPLE_INTERVAL_MS, TimeUnit.MILLISECONDS
//         );

//         Runtime.getRuntime().addShutdownHook(new Thread(this::stop));
//     }

//     private void sampleOnce() {
//         String startTs = sdf.format(new Date());

//         // 直接从执行器拿 4 个分片
//         var shards = FreshnessExecutor.getTraceShards();

//         double[] mb = new double[4];
//         for (int i = 0; i < 4; i++) {
//             if (i < shards.size() && shards.get(i) != null) {
//                 Map<Long, ?> shard = shards.get(i);
//                 mb[i] = estimateMB(shard);
//             } else {
//                 mb[i] = 0.0;
//             }
//         }
//         double totalMB = sumSafe(mb);

//         String endTs = sdf.format(new Date());
//         queue.offer(String.format(Locale.ROOT, "%s,%s,%.2f,%.2f,%.2f,%.2f,%.2f",
//                 startTs, endTs, totalMB, mb[0], mb[1], mb[2], mb[3]));
//     }

//     private static double estimateMB(Map<Long, ?> shard) {
//         if (!USE_RAM_ESTIMATOR || shard == null) return 0.0;
//         try {
//             long bytes = RamUsageEstimator.sizeOfObject(shard);
//             return bytes / 1024.0 / 1024.0;
//         } catch (Throwable t) {
//             return -1.0; // 估算失败，用 -1 标记
//         }
//     }

//     private static double sumSafe(double[] arr) {
//         double s = 0.0;
//         for (double v : arr) if (v > 0) s += v; // -1（失败）不计入
//         return s;
//     }
// }
