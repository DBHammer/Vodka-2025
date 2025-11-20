// 文件：utils/monitor/MemoryMonitor.java
package utils.monitor;

import benchmark.synchronize.tasks.FreshnessExecutor;
import org.apache.lucene.util.RamUsageEstimator;

import java.io.*;
import java.lang.reflect.Field;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.*;

/**
 * 定时监控 FreshnessExecutor.TRACE 的大小与条目数
 * CSV 列：
 * StartTime,FinishTime,Memory(MB),RowCnt,ColumnCnt,VersionCnt,PolicyLminMs,Alpha,HmaxMs
 */
public class MemoryMonitor {

    /* ───────────── 可调参数 ───────────── */
    private static final long SAMPLE_INTERVAL_MS = 1000; // 采样间隔（毫秒）
    private static final boolean USE_RAM_ESTIMATOR = true; // 用 Lucene 估算内存（仅作趋势参考）

    /* 当不使用估算器时的粗略系数（可按 JVM/对象布局调参） */
    private static final int BYTES_PER_ROW = 48;
    private static final int BYTES_PER_COL = 96;
    private static final int BYTES_PER_VER = 56;

    /*
     * ───────────── 反射缓存 ─────────────
     * 需要拿到 FreshnessExecutor.Row 的 "cols" 字段
     * 主类名默认 FreshnessExecutor；若你的工程里还叫 FreshnessExecutorOriginal，
     * 这里做了回退适配。
     */
    private static final Field COLS_FIELD = resolveColsField();

    private static Field resolveColsField() {
        final String[] candidate = new String[] {
                "benchmark.synchronize.tasks.FreshnessExecutor$Row"
        };
        Exception last = null;
        for (String fqcn : candidate) {
            try {
                Class<?> rowCls = Class.forName(fqcn);
                Field f = rowCls.getDeclaredField("cols");
                f.setAccessible(true);
                System.out.println("[MemoryMonitor] Using Row class: " + fqcn + " .field=cols");
                return f;
            } catch (Exception e) {
                last = e;
            }
        }
        throw new ExceptionInInitializerError(last);
    }

    /* ───────────── 成员字段 ───────────── */
    private final File file;
    private final BlockingQueue<String> queue = new LinkedBlockingQueue<>();
    private final ScheduledExecutorService scheduler;
    private final ExecutorService worker;
    private final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private Thread writerThread;

    /* ───────────── 构造 ───────────── */
    public MemoryMonitor(String resultDir) {
        this.file = new File(resultDir, "traceMap_memory.csv");

        scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "TraceMapMonitor-Scheduler");
            t.setDaemon(true);
            return t;
        });

        worker = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "TraceMapMonitor-Worker");
            t.setDaemon(true);
            return t;
        });

        initFile();
    }

    /* ───────────── Public API ───────────── */
    public void monitor() {
        startWriter();
        startSampling();
    }

    public void stop() {
        try {
            scheduler.shutdownNow();
            worker.shutdownNow();
            queue.offer("__POISON__");
            if (writerThread != null)
                writerThread.interrupt();
        } catch (Throwable ignored) {
        }
    }

    /* ───────────── 内部逻辑 ───────────── */
    private void initFile() {
        try {
            if (!file.exists()) {
                File dir = file.getParentFile();
                if (dir != null)
                    dir.mkdirs();
                try (BufferedWriter h = new BufferedWriter(new FileWriter(file, true))) {
                    h.write("StartTime,FinishTime,Memory(MB),RowCnt,ColumnCnt,VersionCnt");
                    h.newLine();
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void startWriter() {
        writerThread = new Thread(() -> {
            try (BufferedWriter out = new BufferedWriter(new FileWriter(file, true))) {
                while (true) {
                    String line = queue.take();
                    if ("__POISON__".equals(line))
                        break;
                    out.write(line);
                    out.newLine();
                    out.flush();
                }
            } catch (InterruptedException ignored) {
            } catch (IOException ioe) {
                ioe.printStackTrace();
            }
        }, "TraceMapMonitor-Writer");
        writerThread.setDaemon(true);
        writerThread.start();
    }

    private void startSampling() {
        scheduler.scheduleAtFixedRate(
                () -> worker.submit(() -> {
                    try {
                        sampleOnce();
                    } catch (Throwable e) {
                        e.printStackTrace();
                    }
                }),
                0, SAMPLE_INTERVAL_MS, TimeUnit.MILLISECONDS);

        Runtime.getRuntime().addShutdownHook(new Thread(this::stop));
    }

    @SuppressWarnings("unchecked")
    private void sampleOnce() throws IllegalAccessException {
        String startTs = sdf.format(new Date());

        // 1) 直接读 FreshnessExecutor 暴露的 TRACE（注意：仅获取引用；遍历时注意 NPE）
        Map<Long, ?> trace = FreshnessExecutor.getTraceMap();

        // 2) 行/列/版本计数
        int rowCnt = trace.size();
        int colCnt = 0;
        int verCnt = 0;

        for (Object rowObj : trace.values()) {
            NavigableMap<Integer, ?>[] cols = (NavigableMap<Integer, ?>[]) COLS_FIELD.get(rowObj);
            if (cols == null)
                continue;
            for (NavigableMap<Integer, ?> vm : cols) {
                if (vm == null)
                    continue;
                colCnt++;
                verCnt += vm.size();
            }
        }

        // 3) 估算内存（建议你画 VersionCnt 对 L 的图来做线性验证；内存仅作参考）
        double mb;
        if (USE_RAM_ESTIMATOR) {
            try {
                // 注意：RamUsageEstimator 对复杂对象图的“深度”估算有偏差，仅作趋势参考
                long bytes = RamUsageEstimator.sizeOfObject(trace);
                mb = bytes / 1024.0 / 1024.0;
            } catch (Throwable e) {
                e.printStackTrace();
                mb = -1;
            }
        } else {
            long bytes = (long) rowCnt * BYTES_PER_ROW
                    + (long) colCnt * BYTES_PER_COL
                    + (long) verCnt * BYTES_PER_VER;
            mb = bytes / 1024.0 / 1024.0;
        }

        // 4) 输出策略参数（方便做横轴 L、可控性上限 Hmax 的分析）

        String endTs = sdf.format(new Date());
        queue.offer(String.format(
                "%s,%s,%.2f,%d,%d,%d",
                startTs, endTs, mb, rowCnt, colCnt, verCnt));
    }
}