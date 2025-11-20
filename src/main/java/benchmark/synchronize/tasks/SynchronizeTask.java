package benchmark.synchronize.tasks;

import static benchmark.oltp.OLTPClient.signalTerminalsRequestEnd;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import bean.Triple;
import benchmark.oltp.DeliveryTracker;
import benchmark.oltp.DeliveryTracker.Transaction;
import benchmark.oltp.OLTPClient;
import benchmark.oltp.OLTPConnection;
import benchmark.oltp.entity.OLTPData;
import benchmark.synchronize.components.HTAPCheckInfo;

public class SynchronizeTask extends Task {
    private final ExecutorService executor;
    private final int checkThreshold = 3;
    private final int dbType;
    private final OLTPConnection db;
    private long sync_time;
    private List<Transaction> txList;

    private static final AtomicLong maxSyncTime = new AtomicLong(0);
    private static final AtomicLong maxXidSync = new AtomicLong(0);

    public static void updateMaxXidSync(long v) {
        maxXidSync.updateAndGet(x -> Math.max(x, v));
    }

    public static void updateMaxValues(long syncTime) {
        maxSyncTime.updateAndGet(x -> Math.max(x, syncTime));
    }

    public SynchronizeTask(int dbType, HTAPCheckInfo htapCheckInfo, OLTPConnection db,
            LinkedHashMap<List<Integer>, Transaction> transactions) {
        this.dbType = dbType;
        this.executor = Executors.newFixedThreadPool(10);
        this.db = db;
    }

    @Override
    public TaskResult runTask(ArrayList<Connection> conns, int threadId) {
        boolean pass = true;
        boolean isApConnErr = false;

        Connection apConnection = conns.get(0);
        Connection tpConnection = conns.get(1);

        System.out.printf("Remain #%d Real-time query%n",
                benchmark.oltp.OLTPClient.htapCheckQueryNumber.decrementAndGet());

        long valueNsMax = 0L;
        long xidNsMax = 0L;

        // 建议本地创建线程池，任务完就关；也可复用外部传进来的 executor
        ExecutorService executor = Executors.newFixedThreadPool(30);

        try {
            txList = new ArrayList<>(OLTPData.taskTrack.getTransactionMap().values());
            List<Future<Long>> verFutures = new ArrayList<>();
            List<Future<Long>> xidFutures = new ArrayList<>();

            AtomicInteger accessedCount = new AtomicInteger(1);
            for (int i = txList.size() - 1; i >= 0 && accessedCount.get() <= checkThreshold; i--) {
                Transaction txn = txList.get(i);
                int taskNo = accessedCount.getAndIncrement();

                System.out.println("Executing for Task: " + taskNo);

                verFutures.add(executor.submit(() -> {
                    try {
                        return new VersionCheckTask(txn, tpConnection, apConnection, taskNo, dbType).call();
                    } finally {
                        System.out.println("Version Task completed");
                    }
                }));

                xidFutures.add(executor.submit(() -> {
                    try {
                        return new XminCheckTask(txn, apConnection, taskNo, dbType).call();
                    } finally {
                        System.out.println("Xmin Task completed");
                    }
                }));
            }

            // 收集结果
            for (Future<Long> f : verFutures)
                valueNsMax = Math.max(valueNsMax, f.get());
            for (Future<Long> f : xidFutures)
                xidNsMax = Math.max(xidNsMax, f.get());

        } catch (Exception e) {
            e.printStackTrace();
            pass = false;
        } finally {
            executor.shutdown();
            try {
                if (!executor.awaitTermination(30, TimeUnit.SECONDS)) {
                    executor.shutdownNow();
                }
            } catch (InterruptedException ie) {
                executor.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }

        // 统计与打印
        double valueSyncMs = valueNsMax / 1_000_000.0;
        double xidSyncMs = xidNsMax / 1_000_000.0;

        OLTPClient.valueSyncList.add(valueSyncMs);
        OLTPClient.xidSyncList.add(xidSyncMs);

        System.out.printf("Value-Sync Latency : %.3f ms%n", valueSyncMs);
        System.out.printf("XID-Sync   Latency : %.3f ms%n", xidSyncMs);

        // 所有任务跑完后再判断是否打印最终报告/结束
        if (benchmark.oltp.OLTPClient.htapCheckQueryNumber.get() <= 0) {
            printDSReport();
        }

        return new TaskResult(taskType, txnCompleteTime, gapTime, startTime, endTime,
                0, pass, isApConnErr, (long) valueSyncMs, (long) xidSyncMs, sync_time, 0, sync_time);
    }

    private void printDSReport() {
        System.out.println("\n=== Synchronization Detail (each run) ===");
        System.out.println("Run\tValue(ms)\tXid(ms)\tRelErr(%)");

        List<Double> valueList = new ArrayList<>(OLTPClient.valueSyncList);
        List<Double> xidList = new ArrayList<>(OLTPClient.xidSyncList);

        // 记录每项的 index → (value, xid, err)
        List<Triple<Integer, Double, Double>> entries = new ArrayList<>();

        for (int i = 0; i < valueList.size(); i++) {
            double v = valueList.get(i);
            double x = xidList.get(i);
            entries.add(new Triple<>(i, v, x));
        }

        // 排除一个最大 value 和一个最小 value
        if (entries.size() > 2) {
            entries.sort(Comparator.comparingDouble(t -> t.getSecond())); // sort by value
            entries.remove(0); // remove min
            entries.remove(entries.size() - 1); // remove max
            entries.remove(entries.size() - 1); // remove max
        }

        double sumVal = 0, sumXid = 0, sumErr = 0;
        int run = 1;
        for (Triple<Integer, Double, Double> entry : entries) {
            double v = entry.getSecond();
            double x = entry.getThird();
            double err = v == 0 ? 0 : Math.abs(x - v) * 100.0 / v;
            System.out.printf("%3d\t%9.2f\t%9.2f\t%8.3f\n", run++, v, x, err);
            sumVal += v;
            sumXid += x;
        }

        int n = entries.size();
        System.out.println("--- Avg (without max/min) ---");
        System.out.printf("Avg Value(ms): %.2f\n", sumVal / n);
        System.out.printf("Avg  Xid(ms): %.2f\n", sumXid / n);
        if (sumXid != 0) {
            double rel = Math.abs(sumXid - sumVal) / sumXid * 100.0;
            System.out.printf("Avg RelErr(%%): %.3f%n", rel); // 这里用 %%
        } else {
            System.out.println("Avg RelErr(%): N/A (sumXid == 0)");
        }
        signalTerminalsRequestEnd(false);
    }
}