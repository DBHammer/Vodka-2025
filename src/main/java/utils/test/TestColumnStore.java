package utils.test;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

/**
 * Compare rowstore vs columnstore TPC-H query latency and compute speedup
 * ratios.
 *
 * Phase A: run queries on rowstore.
 * Phase B: drop foreign keys, create column-store copies (suffix _column),
 * run queries on those copies.
 * Results: CSV with row_ms, col_ms, speedup_ratio.
 */
public class TestColumnStore {

    private static final String JDBC_URL = "jdbc:postgresql://49.52.27.33:5532/benchmarksql";
    private static final String USER = "postgres";
    private static final String PASSWORD = "postgres";
    private final Set<String> skipListRow;
    private final Set<String> skipListCol;

    private static final Path SQL_DIR_ROW = Paths.get("/home/xjk/hzr/revision/Vodka-Benchmark/run/tpchSQL/");
    private static final Path SQL_DIR_COLUMN = Paths.get("/home/xjk/hzr/revision/Vodka-Benchmark/run/tpchSQL/column");
    private static final Path RESULT_DIR = Paths
            .get("/home/xjk/hzr/revision/Vodka-Benchmark/run/tpchSQL/column/results");
    private static final Path SKIP_FILE_ROW = Paths
            .get("/home/xjk/hzr/revision/Vodka-Benchmark/run/tpchSQL/skip_list_row.txt");
    private static final Path SKIP_FILE_COL = Paths
            .get("/home/xjk/hzr/revision/Vodka-Benchmark/run/tpchSQL/skip_list_col.txt");
    private static final DateTimeFormatter TS_FMT = DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss");

    private final Connection conn;
    private final List<Path> rowSqlFiles;
    private final List<Path> colSqlFiles;

    public TestColumnStore(Connection conn) throws SQLException, IOException {
        this.conn = conn;
        this.rowSqlFiles = loadSqlFiles(SQL_DIR_ROW);
        this.colSqlFiles = loadSqlFiles(SQL_DIR_COLUMN);
        this.skipListRow = loadSkipList(SKIP_FILE_ROW);
        this.skipListCol = loadSkipList(SKIP_FILE_COL);
        System.out.println("Session initialized");
        try (Statement st = conn.createStatement()) {
            st.execute("SET max_parallel_workers_per_gather = 0");
            // st.execute("SET synchronous_commit = OFF;");
            st.execute("SET citus.max_adaptive_executor_pool_size = 1;");
            // st.execute("SET citus.enable_repartition_joins = ON;");
        }
        System.out.println("Session: max_parallel_workers_per_gather = 0");
        // SELECT
        // pg_terminate_backend(pid)
        // FROM
        // pg_stat_activity
        // WHERE
        // datname = 'benchmarksql'
        // AND pid <> pg_backend_pid();
    }

    private Set<String> loadSkipList(Path file) throws IOException {
        if (!Files.exists(file))
            return Collections.emptySet();
        return Files.readAllLines(file).stream()
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .collect(Collectors.toSet());
    }

    private List<Path> loadSqlFiles(Path dir) throws IOException {
        return Files.list(dir)
                .filter(p -> p.toString().endsWith(".sql"))
                .sorted()
                .collect(Collectors.toList());
    }

    public static void main(String[] args) throws Exception {
        Files.createDirectories(RESULT_DIR);

        try (Connection c = DriverManager.getConnection(JDBC_URL, USER, PASSWORD)) {
            TestColumnStore bench = new TestColumnStore(c);

            // Phase A: Rowstore
            long[] rowTimes = bench.runOnce("rowstore", bench.rowSqlFiles);

            // Phase B: Drop FKs & build column-store copies
            bench.createColumnTables();
            long[] colTimes = bench.runOnce("columnar", bench.colSqlFiles);
            // Write results
            bench.writeSpeedupCsv(rowTimes, colTimes);
        }
    }

    /**
     * Executes all SQL files in `files`, records latency (ms) per file.
     */
    private long[] runOnce(String tag, List<Path> files) throws IOException, SQLException {
        System.out.println("=== Phase: " + tag + " ===");
        int Q = files.size();
        long[] times = new long[Q];
        for (int i = 0; i < Q; i++) {
            String name = files.get(i).getFileName().toString();
            if (tag.equals("rowstore") && skipListRow.contains(name)) {
                System.out.printf("⏭ Skipping %s (in skip list)%n", name);
                times[i] = -1;
                continue;
            }
            if (tag.equals("columnar") && skipListCol.contains(name)) {
                System.out.printf("⏭ Skipping %s (in skip list)%n", name);
                times[i] = -1;
                continue;
            }
            System.out.println("executing " + name);
            String sql = Files.readString(files.get(i), StandardCharsets.UTF_8);
            long t0 = System.nanoTime();
            try (PreparedStatement ps = conn.prepareStatement(sql);
                    ResultSet rs = ps.executeQuery()) {
            } catch (Exception e) {
                e.printStackTrace();
                continue;
            }
            long ms = (System.nanoTime() - t0) / 1_000_000;
            times[i] = ms;
            System.out.printf(" %s: %.3f ms%n", files.get(i).getFileName(), (double) ms);
        }
        return times;
    }

    /**
     * Creates column-store copies of tables with `_col` suffix:
     * - Dimension/table-of-small-size 用 reference-table
     * - Fact/table-of-large-size 用 distributed-table，并且 colocate 在 warehouse_col 上
     */
    /**
     * Creates column‐store copies of all tables:
     * - referenceTables 会被 create_reference_table
     * - distributedTables 会被 create_distributed_table(..., distKey[, colocate_with
     * => 'vodka_warehouse_col'])
     * 并行执行、逐表打印进度和耗时。
     */
    private void createColumnTables() throws SQLException, InterruptedException {
        System.out.println("Creating column-store copies in parallel...");

        // 1) 定义所有要处理的表
        var allTables = List.of(
                "vodka_region", "vodka_nation",
                "vodka_item", "vodka_supplier", "vodka_customer", "vodka_oorder",
                "vodka_order_line", "vodka_stock");
        // var allTables = List.of(
        // "vodka_stock");

        // 2) 指定哪些是 Reference（全表复制）
        // Set<String> referenceTables = Set.of(
        // "vodka_region", "vodka_nation", "vodka_supplier");
        Set<String> referenceTables = Set.of(
                "vodka_region", "vodka_nation",
                "vodka_item", "vodka_supplier", "vodka_customer", "vodka_oorder", "vodka_stock");

        // Map<String, String> distMap = Map.ofEntries();
        // 3) 指定哪些是 Distributed，以及它们的分片列
        Map<String, String> distMap = Map.ofEntries(
                Map.entry("vodka_order_line", "ol_w_id"));
        // Map.entry("vodka_stock", "s_w_id");

        // // 4) 为需要 colocate 的表指定 colocate_with
        // Set<String> colocateWithWarehouse = Set.of(
        // "vodka_district","vodka_customer",
        // "vodka_history", "vodka_new_order",
        // "vodka_oorder", "vodka_order_line",
        // "vodka_stock"
        // );

        ExecutorService exec = Executors.newFixedThreadPool(allTables.size());
        List<Future<?>> futures = new ArrayList<>();
        java.util.concurrent.atomic.AtomicInteger done = new java.util.concurrent.atomic.AtomicInteger();

        for (String tbl : allTables) {
            futures.add(exec.submit(() -> {
                String tblCol = tbl + "_col";
                System.out.printf("%s ➜ starting %s%n",
                        Thread.currentThread().getName(), tblCol);
                try (Connection c2 = DriverManager.getConnection(JDBC_URL, USER, PASSWORD);
                        Statement st = c2.createStatement()) {

                    // DROP 旧表
                    st.execute("DROP TABLE IF EXISTS " + tblCol);
                    // CREATE TABLE vodka_supplier_col2 (LIKE vodka_supplier INCLUDING DEFAULTS EXCLUDING CONSTRAINTS) USING columnar;
                    // SELECT create_distributed_table('vodka_supplier_col2', 's_suppkey');
                    // INSERT INTO vodka_supplier_col2 SELECT * FROM vodka_supplier_col2;
                    // 1) CREATE COLUMNAR TABLE
                    st.execute(String.format(
                            "CREATE TABLE %s (LIKE %s INCLUDING DEFAULTS EXCLUDING CONSTRAINTS) USING columnar",
                            tblCol, tbl));
                    System.out.println("create done");

                    // 2) REGISTER reference 或 distributed
                    if (referenceTables.contains(tbl)) {
                        st.execute("SELECT create_reference_table('" + tblCol + "')");
                    } else {
                        String distKey = distMap.get(tbl);
                        st.execute(String.format(
                                "SELECT create_distributed_table('%s','%s')",
                                tblCol, distKey));
                    }

                    // 3) 批量 INSERT 并测时
                    long t0 = System.nanoTime();
                    st.execute(String.format(
                            "INSERT INTO %s SELECT * FROM %s", tblCol, tbl));
                    long ms = (System.nanoTime() - t0) / 1_000_000;
                    System.out.printf("✅ Loaded %s in %d ms%n", tblCol, ms);

                    // 4) 进度
                    int n = done.incrementAndGet();
                    System.out.printf("[%d/%d] DONE %s%n",
                            n, allTables.size(), tblCol);

                } catch (SQLException ex) {
                    throw new RuntimeException("Error on " + tbl, ex);
                }
            }));
        }

        exec.shutdown();
        for (Future<?> f : futures) {
            try {
                f.get();
            } catch (ExecutionException e) {
                throw new RuntimeException("Parallel creation error", e.getCause());
            }
        }
        System.out.println("🎉 All column-store copies created.");
    }

    /**
     * Outputs CSV: query,row_ms,col_ms,speedup_ratio
     */
    private void writeSpeedupCsv(long[] rowTimes, long[] colTimes) throws IOException {
        String ts = TS_FMT.format(LocalDateTime.now());
        Path out = RESULT_DIR.resolve("tpch_speedup_" + ts + ".csv");
        try (BufferedWriter bw = Files.newBufferedWriter(out)) {
            bw.write("query,row_ms,col_ms,speedup_ratio\n");
            for (int i = 0; i < rowSqlFiles.size(); i++) {
                String name = rowSqlFiles.get(i).getFileName().toString();
                double ratio = (double) rowTimes[i] / colTimes[i];
                bw.write(String.format("%s,%.3f,%.3f,%.2f%n",
                        name, (double) rowTimes[i], (double) colTimes[i], ratio));
            }
        }
        System.out.println("Speedup CSV → " + out);
    }
}