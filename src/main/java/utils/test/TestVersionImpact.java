package utils.test;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.sql.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Compare the impact of version columns on TPC-H query latency.
 *
 * - Phase A (“before_drop”) runs selected queries on one instance / directory.
 * - Optional pause (e.g., restart DB, drop OS cache).
 * - Phase B (“after_drop”) drops access_version columns, runs the same queries
 * on another instance / directory, then adds the columns back.
 *
 * © 2025 周呆宝
 */
public class TestVersionImpact {

    /* ── JDBC & path config ─────────────────────────────────────────── */
    private static final String JDBC_URL_BEFORE = "jdbc:postgresql://49.52.27.33:5532/benchmarksql";
    private static final String JDBC_URL_AFTER = "jdbc:postgresql://49.52.27.33:5532/benchmarksql";

    private static final String USER = "postgres";
    private static final String PASSWORD = "postgres";

    /** put q1.sql, q2.sql … here (any subset). */
    private static final Path DIR_BEFORE = Paths.get("/home/xjk/hzr/revision/Vodka-Benchmark/run/tpchSQL/test/test");
    private static final Path DIR_AFTER = Paths.get("/home/xjk/hzr/revision/Vodka-Benchmark/run/tpchSQL/test/test");

    private static final Path RESULT_DIR = Paths.get("/home/xjk/hzr/revision/Vodka-Benchmark/run/tpchSQL/test/");

    /* ── util ───────────────────────────────────────────────────────── */
    private static final DateTimeFormatter TS_FMT = DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss");

    /* per-session */
    private final Connection conn;
    private final Path sqlDir;

    private TestVersionImpact(Connection conn, Path sqlDir) throws SQLException {
        this.conn = conn;
        this.sqlDir = sqlDir;
        try (Statement st = conn.createStatement()) {
            st.execute("SET max_parallel_workers_per_gather = 1");
        }
        System.out.println("Session: max_parallel_workers_per_gather = 1");
    }

    /* ───────────────────────────────────────────────────────────────── */
    public static void main(String[] args) throws Exception {

        /* ===== Phase A: BEFORE DROP ===== */
        try (Connection c = DriverManager.getConnection(
                JDBC_URL_BEFORE, USER, PASSWORD)) {
            TestVersionImpact bench = new TestVersionImpact(c, DIR_BEFORE);
            bench.runOnce("before_drop");
        }

        /* ===== Phase B: AFTER DROP ===== */
        try (Connection c = DriverManager.getConnection(
                JDBC_URL_AFTER, USER, PASSWORD)) {
            TestVersionImpact bench = new TestVersionImpact(c, DIR_AFTER);
            bench.dropVersionColumns();
            bench.runOnce("after_drop");
            bench.addVersionColumns();
        }
    }

    /* ── core runner ───────────────────────────────────────────────── */
    private void runOnce(String tag) throws IOException, SQLException {

        List<Path> sqlFiles = Files.list(sqlDir)
                .filter(p -> p.toString().endsWith(".sql"))
                .sorted() // q1.sql, q2.sql, …
                .collect(Collectors.toList());

        final int Q = sqlFiles.size();
        final int ROUNDS = 1; // change if needed
        long[][] lat = new long[Q][ROUNDS];

        for (int r = 0; r < ROUNDS; r++) {
            System.out.printf("[%s] round %d/%d%n", tag, r + 1, ROUNDS);
            for (int i = 0; i < Q; i++) {
                String sql = Files.readString(sqlFiles.get(i), StandardCharsets.UTF_8);
                long t0 = System.nanoTime();
                try (PreparedStatement ps = conn.prepareStatement(sql);
                        ResultSet rs = ps.executeQuery()) {
                    /* consume */ }
                long ms = (System.nanoTime() - t0) / 1_000_000;
                lat[i][r] = ms;
                String name = sqlFiles.get(i).getFileName().toString();
                System.out.printf("   %s finished in %.3f ms%n", name, (double) ms);
            }
        }

        Files.createDirectories(RESULT_DIR);
        Path out = RESULT_DIR.resolve(
                "tpch_" + tag + "_" + TS_FMT.format(LocalDateTime.now()) + ".csv");

        try (BufferedWriter bw = Files.newBufferedWriter(out)) {
            bw.write("query,round1_ms\n");
            for (int i = 0; i < Q; i++) {
                double avg = lat[i][0];
                bw.write(String.format("%s,%.3f%n",
                        sqlFiles.get(i).getFileName(), avg));
            }
        }
        System.out.println("[" + tag + "] CSV → " + out);
    }

    /* ── helpers: pause / DDL ──────────────────────────────────────── */
    private static void pauseForColdStart() {
        System.out.println("\n⏸  Pause 60 s – restart DB / drop caches if desired …");
        try {
            Thread.sleep(60_000);
        } catch (InterruptedException ignored) {
        }
    }

    private void dropVersionColumns() throws SQLException {
        String[] ddl = {
                "ALTER TABLE vodka_order_line DROP COLUMN IF EXISTS access_version",
                "ALTER TABLE vodka_oorder DROP COLUMN IF EXISTS access_version",
                "ALTER TABLE vodka_customer DROP COLUMN IF EXISTS access_version",
                "ALTER TABLE vodka_stock DROP COLUMN IF EXISTS access_version"
        };
        execDDL(ddl, "Dropped access_version columns");
    }

    private void addVersionColumns() throws SQLException {
        String[] ddl = {
                "ALTER TABLE vodka_order_line " +
                        "  ADD COLUMN IF NOT EXISTS access_version integer NOT NULL DEFAULT 0",
                "ALTER TABLE vodka_oorder " +
                        "  ADD COLUMN IF NOT EXISTS access_version integer NOT NULL DEFAULT 0",
                "ALTER TABLE vodka_customer " +
                        "  ADD COLUMN IF NOT EXISTS access_version integer NOT NULL DEFAULT 0",
                "ALTER TABLE vodka_stock " +
                        "  ADD COLUMN IF NOT EXISTS access_version integer NOT NULL DEFAULT 0"
        };
        execDDL(ddl, "Re-added access_version columns (DEFAULT 0)");
    }

    private void execDDL(String[] ddl, String doneMsg) throws SQLException {
        try (Statement st = conn.createStatement()) {
            for (String sql : ddl) {
                System.out.println("Executing: " + sql);
                st.execute(sql);
            }
        }
        System.out.println(doneMsg + "\n");
    }
}