

package utils.test;

import java.sql.*;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;

/**
 * 并发 HTAP 延迟测试：
 * - 每轮同时启动 CONCURRENCY 个插入线程
 * - 每个线程写 BULK_SIZE 行，最后一行带 tag
 * - 为每个 tag 派 3 个 watcher，测 value / xmin / commit-ts 延迟
 */
public class HTAPDelayConcurrent2 {

    // ─────── 基本配置 ───────────────────────────────────────────────
    static final String MASTER_URL = "jdbc:postgresql://49.52.27.33:5532/test";
    static final String STANDBY_URL = "jdbc:postgresql://49.52.27.35:5532/test";
    static final String USER = "postgres";
    static final String PASSWORD = "";
    static final String TABLE_NAME = "t1";

    // 每线程一次写多少行
    static final int BULK_SIZE = 100_000;
    // 并发插入线程数
    static final int CONCURRENCY = 10;
    // 总轮次
    static final int REPEAT = 10;

    // watcher 相关
    static final long CHECK_INTERVAL_MS = 1;
    static final long TIMEOUT_MS = 600_000;

    // ─────── 数据结构 ───────────────────────────────────────────────
    record InsertInfo(String tag, int xid, Instant commitInstant) {
    }

    record Result(double value, Double xmin, Double commit,
            Double xminErr, Double commitErr) {
    }

    // ──────── 主入口 ───────────────────────────────────────────────
    public static void main(String[] args) throws Exception {
        System.out.println("准备表...");
        prepareTable();

        ExecutorService insertPool = Executors.newFixedThreadPool(CONCURRENCY);
        ExecutorService watcherPool = Executors.newCachedThreadPool();

        for (int round = 1; round <= REPEAT; round++) {
            System.out.printf("%n=== Round %d (并发 %d 插入) ===%n", round, CONCURRENCY);

            // 1. 同步启动 CONCURRENCY 个插入 Callable
            List<Future<InsertInfo>> futures = new ArrayList<>();
            for (int i = 0; i < CONCURRENCY; i++) {
                futures.add(insertPool.submit(() -> bulkInsertOnce()));
            }

            // 2. 收集 InsertInfo 并为每个 tag 安排 3 个 watcher
            List<Result> allResults = new ArrayList<>();
            CountDownLatch roundLatch = new CountDownLatch(CONCURRENCY * 3);

            for (Future<InsertInfo> fut : futures) {
                InsertInfo info = fut.get(); // 等待单个插入完成
                Instant t0 = info.commitInstant;

                Map<String, Instant> times = new ConcurrentHashMap<>();

                watcherPool.execute(() -> waitValueVisible(info.tag, times, roundLatch, t0));
                watcherPool.execute(() -> waitXminMatch(info.tag, info.xid, times, roundLatch, t0));
                watcherPool.execute(() -> waitCommitTimestamp(info.xid, times, roundLatch, t0));

                // 另开一个收集线程：等 3 watcher 结束后统计
                watcherPool.execute(() -> {
                    try {
                        boolean ok = roundLatch.await(TIMEOUT_MS, TimeUnit.MILLISECONDS);
                        if (!ok || !times.containsKey("value"))
                            return; // timeout
                        double valueDelay = Duration.between(t0, times.get("value")).toMillis();
                        Double xminDelay = times.containsKey("xmin")
                                ? (double) Duration.between(t0, times.get("xmin")).toMillis()
                                : null;
                        Double commitDelay = times.containsKey("commit")
                                ? (double) Duration.between(t0, times.get("commit")).toMillis()
                                : null;
                        Double xminErr = xminDelay == null ? null : Math.abs(xminDelay - valueDelay) / valueDelay;
                        Double commitErr = commitDelay == null ? null : Math.abs(commitDelay - valueDelay) / valueDelay;
                        synchronized (allResults) {
                            allResults.add(new Result(valueDelay, xminDelay, commitDelay, xminErr, commitErr));
                        }
                    } catch (InterruptedException ignored) {
                    }
                });
            }

            // 3. 等这一轮全部 watcher 结束
            watcherPool.shutdown();
            watcherPool.awaitTermination(TIMEOUT_MS + 5_000, TimeUnit.MILLISECONDS);
            watcherPool = Executors.newCachedThreadPool(); // 新起 watcher 池供下一轮

            // 4. 输出本轮结果
            System.out.println("idx\tValue(ms)\tXmin(ms)\tCommit(ms)\tXminErr\t\tCommitErr");
            int idx = 1;
            for (Result r : allResults) {
                System.out.printf("%3d\t%9.2f\t", idx++, r.value);
                System.out.printf("%9s\t",
                        r.xmin != null ? String.format("%.2f", r.xmin) : "timeout");
                System.out.printf("%9s\t",
                        r.commit != null ? String.format("%.2f", r.commit) : "timeout");

                if (r.xminErr != null) {
                    System.out.printf("%.4f\t\t", r.xminErr);
                } else {
                    System.out.print("(timeout)\t\t");
                }

                if (r.commitErr != null) {
                    System.out.printf("%.4f%n", r.commitErr);
                } else {
                    System.out.println("(timeout)");
                }
            }
        }

        insertPool.shutdownNow();
        System.out.println("\n测试完成.");
    }

    // ─────── Prepare table ─────────────────────────────────────────
    static void prepareTable() throws Exception {
        try (Connection conn = DriverManager.getConnection(MASTER_URL, USER, PASSWORD);
                Statement stmt = conn.createStatement()) {
            stmt.executeUpdate("CREATE TABLE IF NOT EXISTS " + TABLE_NAME + " (" +
                    "id SERIAL PRIMARY KEY," +
                    "data TEXT," +
                    "tag  TEXT UNIQUE," +
                    "inserted_at TIMESTAMPTZ DEFAULT now())");
            stmt.executeUpdate("TRUNCATE " + TABLE_NAME);
        }
    }

    // ─────── 单线程插入 BULK_SIZE 行，返回 xid + tag ────────────────
    static InsertInfo bulkInsertOnce() throws Exception {
        String tag = "sync-" + UUID.randomUUID();
        try (Connection conn = DriverManager.getConnection(MASTER_URL, USER, PASSWORD);
                Statement stmt = conn.createStatement()) {
            conn.setAutoCommit(false);

            StringBuilder sb = new StringBuilder("INSERT INTO ").append(TABLE_NAME).append(" (data, tag) VALUES ");
            for (int i = 1; i < BULK_SIZE; i++) {
                sb.append("('val_").append(i).append("', NULL),");
            }
            sb.append("('val_last', '").append(tag).append("') RETURNING xmin;");

            ResultSet rs = stmt.executeQuery(sb.toString());
            int xid = -1;
            while (rs.next())
                xid = rs.getInt(1);

            stmt.execute("COMMIT;");
            return new InsertInfo(tag, xid, Instant.now());
        }
    }

    // ─────── value watcher ─────────────────────────────────────────
    static void waitValueVisible(String tag, Map<String, Instant> res,
            CountDownLatch latch, Instant t0) {
        try (Connection conn = DriverManager.getConnection(STANDBY_URL, USER, PASSWORD);
                PreparedStatement ps = conn.prepareStatement(
                        "SELECT data FROM " + TABLE_NAME + " WHERE tag = ?")) {
            while (Duration.between(t0, Instant.now()).toMillis() < TIMEOUT_MS) {
                ps.setString(1, tag);
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next() && "val_last".equals(rs.getString(1))) {
                        res.put("value", Instant.now());
                        break;
                    }
                }
                Thread.sleep(CHECK_INTERVAL_MS);
            }
        } catch (Exception ignored) {
        }
        latch.countDown();
    }

    // ─────── xmin watcher ──────────────────────────────────────────
    static void waitXminMatch(String tag, int expectedXid,
            Map<String, Instant> res, CountDownLatch latch, Instant t0) {
        try (Connection conn = DriverManager.getConnection(STANDBY_URL, USER, PASSWORD);
                PreparedStatement ps = conn.prepareStatement(
                        "SELECT xmin FROM " + TABLE_NAME + " WHERE tag = ?")) {
            while (Duration.between(t0, Instant.now()).toMillis() < TIMEOUT_MS) {
                ps.setString(1, tag);
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next() && rs.getInt(1) == expectedXid) {
                        res.put("xmin", Instant.now());
                        break;
                    }
                }
                Thread.sleep(CHECK_INTERVAL_MS);
            }
        } catch (Exception ignored) {
        }
        latch.countDown();
    }

    // ─────── commit-timestamp watcher ──────────────────────────────
    static void waitCommitTimestamp(int xid, Map<String, Instant> res,
            CountDownLatch latch, Instant t0) {
        final String sql = "SELECT pg_xact_commit_timestamp(" + "'" + xid + "'"+ ")";;
        try (Connection conn = DriverManager.getConnection(STANDBY_URL, USER, PASSWORD);
                Statement st = conn.createStatement()) {
            while (Duration.between(t0, Instant.now()).toMillis() < TIMEOUT_MS) {
                try (ResultSet rs = st.executeQuery(sql)) {
                    if (rs.next() && rs.getTimestamp(1) != null) {
                        res.put("commit", Instant.now());
                        break;
                    }
                }
                Thread.sleep(CHECK_INTERVAL_MS);
            }
        } catch (Exception ignored) {
        }
        latch.countDown();
    }
}