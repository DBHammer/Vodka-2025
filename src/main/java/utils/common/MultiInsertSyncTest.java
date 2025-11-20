package utils.common;

import java.sql.*;
import java.time.Duration;
import java.time.Instant;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

public class MultiInsertSyncTest {

    /* ---------------- 配置 ---------------- */
    private static final String MASTER_URL = "jdbc:postgresql://49.52.27.33:5532/test";
    private static final String STANDBY_URL = "jdbc:postgresql://49.52.27.35:5532/test";
    private static final Properties DB_PROPS = new Properties();
    static {
        DB_PROPS.setProperty("user", "postgres");
        DB_PROPS.setProperty("password", "");
    }

    private static final String TABLE_NAME = "multi_insert_test";
    private static final int TEST_ROUNDS = 20;
    private static final long TIMEOUT_MS = 60_000;
    private static final long CHECK_INTERVAL_MS = 0; // 0 == 紧凑轮询

    /* ---------------- 主流程 ---------------- */
    public static void main(String[] args) throws Exception {

        /* 建表 */
        try (Connection c = DriverManager.getConnection(MASTER_URL, DB_PROPS);
                Statement s = c.createStatement()) {
            s.execute("DROP TABLE IF EXISTS " + TABLE_NAME);
            s.execute("CREATE TABLE " + TABLE_NAME +
                    " (id SERIAL PRIMARY KEY, data TEXT, " +
                    "  created_at TIMESTAMP DEFAULT now())");
        }

        System.out.printf(">>> 开始 %d 轮同步延迟测试%n%n", TEST_ROUNDS);

        for (int round = 1; round <= TEST_ROUNDS; round++) {

            /* ---------- 1. 主库插入并取得 LSN / commit_ts ---------- */
            int insertedId;
            String baselineLsn;
            Timestamp commitTime;

            try (Connection conn = DriverManager.getConnection(MASTER_URL, DB_PROPS)) {
                conn.setAutoCommit(false);

                try (PreparedStatement ins = conn.prepareStatement(
                        "INSERT INTO " + TABLE_NAME + "(data) VALUES (?)",
                        Statement.RETURN_GENERATED_KEYS);
                        PreparedStatement lsn = conn.prepareStatement(
                                "SELECT pg_current_wal_lsn()")) {

                    ins.setString(1, "Round " + round);
                    ins.executeUpdate();
                    ResultSet keys = ins.getGeneratedKeys();
                    keys.next();
                    insertedId = keys.getInt(1);

                    ResultSet lsnRs = lsn.executeQuery();
                    lsnRs.next();
                    baselineLsn = lsnRs.getString(1);

                    conn.commit(); // 真正落盘
                }

                try (Statement st = conn.createStatement();
                        ResultSet tsRs = st.executeQuery("SELECT now()")) {
                    tsRs.next();
                    commitTime = tsRs.getTimestamp(1);
                }
            }

            /* ---------- 2. 并发监控 ---------- */
            Instant begin = Instant.now();

            AtomicReference<Duration> walDelay = new AtomicReference<>();
            AtomicReference<Duration> visDelay = new AtomicReference<>();
            AtomicReference<Duration> tsDelay = new AtomicReference<>();

            CountDownLatch walDone = new CountDownLatch(1);

            /* --- 2.1 WAL 回放监控 --- */
            Thread walMonitor = new Thread(() -> {
                try (Connection standby = DriverManager.getConnection(STANDBY_URL, DB_PROPS);
                        PreparedStatement ps = standby.prepareStatement(
                                "SELECT pg_last_wal_replay_lsn() >= ?::pg_lsn")) {

                    while (Duration.between(begin, Instant.now()).toMillis() < TIMEOUT_MS) {
                        ps.setString(1, baselineLsn);
                        ResultSet rs = ps.executeQuery();
                        rs.next();
                        if (rs.getBoolean(1)) {
                            walDelay.set(Duration.between(begin, Instant.now()));
                            walDone.countDown(); // 通知可见性线程
                            return;
                        }
                        Thread.sleep(CHECK_INTERVAL_MS);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    walDone.countDown(); // 超时也要释放
                }
            });

            /* --- 2.2 数据可见性监控 --- */
            Thread visMonitor = new Thread(() -> {
                try (Connection standby = DriverManager.getConnection(STANDBY_URL, DB_PROPS);
                        PreparedStatement qry = standby.prepareStatement(
                                "SELECT 1 FROM " + TABLE_NAME + " WHERE id = ?")) {

                    qry.setInt(1, insertedId);

                    walDone.await(); // 等 WAL 完成

                    // —— 二次快速检查 ——
                    if (rowVisible(qry)) {
                        visDelay.set(Duration.ZERO); // 已可见：延迟 0
                        return;
                    }

                    // 继续轮询直到可见
                    Instant afterWal = Instant.now();
                    while (Duration.between(afterWal, Instant.now()).toMillis() < TIMEOUT_MS) {
                        if (rowVisible(qry)) {
                            visDelay.set(Duration.between(afterWal, Instant.now()));
                            return;
                        }
                        Thread.sleep(CHECK_INTERVAL_MS);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });

            /* --- 2.3 pg_last_xact_replay_timestamp() 监控 --- */
            Thread tsMonitor = new Thread(() -> {
                try (Connection standby = DriverManager.getConnection(STANDBY_URL, DB_PROPS);
                        PreparedStatement ps = standby.prepareStatement(
                                "SELECT pg_last_xact_replay_timestamp()")) {

                    while (Duration.between(begin, Instant.now()).toMillis() < TIMEOUT_MS) {
                        ResultSet rs = ps.executeQuery();
                        rs.next();
                        Timestamp ts = rs.getTimestamp(1);
                        if (ts != null && !ts.before(commitTime)) {
                            tsDelay.set(Duration.between(begin, Instant.now()));
                            return;
                        }
                        Thread.sleep(CHECK_INTERVAL_MS);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });

            /* ---------- 3. 执行 & 汇报 ---------- */
            walMonitor.start();
            visMonitor.start();
            // tsMonitor.start();

            walMonitor.join();
            visMonitor.join();
            // tsMonitor.join();

            Duration d1 = walDelay.get();
            Duration d2 = visDelay.get();
            // Duration d3 = tsDelay.get();

            if (d1 != null && d2 != null) {
                System.out.printf("Round %2d | WAL: %.3f s | VC: %.3f s | TS: %.3f s%n",
                        round, d1.toMillis() / 1000.0, d2.toMillis() / 1000.0, d2.toMillis() / 1000.0);
            } else {
                System.out.printf("⚠ Round %d 超时或监控失败%n", round);
            }
        }
    }

    /* 帮助函数：检查行是否存在 */
    private static boolean rowVisible(PreparedStatement ps) throws SQLException {
        try (ResultSet rs = ps.executeQuery()) {
            return rs.next();
        }
    }
}