package benchmark.olap;

import org.json.JSONArray;
import org.json.JSONObject; // 确保你导入了这个库
import org.json.JSONTokener;

import benchmark.olap.query.*;
import benchmark.oltp.OLTPClient;
import benchmark.oltp.schedule.OnlineDDLCoordinator;
import config.CommonConfig;
import org.apache.log4j.Logger;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.sql.*;
import java.sql.Date;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class OLAPTerminal implements Runnable {
    public static AtomicLong oorderTableSize = new AtomicLong(benchmark.olap.query.baseQuery.orderOriginSize); // 通过查询获得的oorder表的实时大小
    public static AtomicLong orderLineTableSize = new AtomicLong(benchmark.olap.query.baseQuery.olOriginSize); // 通过查询获得的orderline表的实时大小
    public static AtomicLong orderlineTableNotNullSize = new AtomicLong(benchmark.olap.query.baseQuery.olNotnullSize);
    public static AtomicLong orderlineTableRecipDateNotNullSize = new AtomicLong(
            benchmark.olap.query.baseQuery.olNotnullSize);
    public static boolean filterRateCheck = false; // 为 TRUE 时获取过滤比分母查询
    public static boolean countPlan = false; // 为 TRUE 时记查询计划
    public static boolean detailedPlan = true; // 为 TRUE 以 json 格式保存查询计划
    private static final Logger log = Logger.getLogger(OLAPTerminal.class);
    private final int interval;
    private final int dynamicParam;
    private Connection conn;
    private final int dbType;
    private volatile boolean stopRunningSignal = false;
    private String terminalName;
    private final String resultDirName;
    private static int queryNumber = 22;
    private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private final String parallel_sql;
    private static final String origin_sql = "set _force_parallel_query_dop = 1;";
    private final boolean parallelSwitch;
    private final boolean shuffleQueries;
    private Thread myThread;
    private int threadID;
    // private static final String[] sqlPath = { "tpchSQL/5.sql", "tpchSQL/8.sql" };
    private static final String[] sqlPath = { "tpchSQL/1.sql", "tpchSQL/2.sql",
            "tpchSQL/3.sql", "tpchSQL/4.sql", "tpchSQL/5.sql",
            "tpchSQL/6.sql", "tpchSQL/7.sql", "tpchSQL/8.sql", "tpchSQL/9.sql",
            "tpchSQL/10.sql", "tpchSQL/11.sql", "tpchSQL/12.sql", "tpchSQL/13.sql", "tpchSQL/14.sql", "tpchSQL/15.sql",
            "tpchSQL/16.sql", "tpchSQL/17.sql", "tpchSQL/18.sql", "tpchSQL/19.sql", "tpchSQL/20.sql",
            "tpchSQL/21.sql", "tpchSQL/22.sql" };
    private final TxnNumRecord txnNumRecord;

    public Thread getThread() {
        return myThread;
    }

    public OLAPTerminal(String database, Properties dbProps, int dbType, int interval, OLTPClient parent,
            int dynamicParam, boolean parallelSwitch, int isolation_level, int parallel_degree, String iresultDirName,
            int threadID)
            throws SQLException {
        this.dbType = dbType;
        this.resultDirName = iresultDirName;
        this.interval = interval;
        this.conn = DriverManager.getConnection(database, dbProps);
        this.conn.setAutoCommit(false);
        this.dynamicParam = dynamicParam;
        this.txnNumRecord = new TxnNumRecord(conn, dbType);
        this.parallelSwitch = parallelSwitch;
        this.parallel_sql = setQueryParalleDegreelByDBType(dbType, parallel_degree);
        isolation_level = 2; // default configuration
        this.shuffleQueries = true; // 是否打乱查询
        this.threadID = threadID;
        switch (isolation_level) {
            case 0 -> conn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
            case 2 -> conn.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
            case 3 -> conn.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
            default -> conn.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
        }
    }

    public static String setQueryParalleDegreelByDBType(int dbType, int parallel_degree) {
        String result = "";
        switch (dbType) {
            case CommonConfig.DB_OCEANBASE -> result = "set _force_parallel_query_dop = " + parallel_degree + " ; ";
            case CommonConfig.DB_TIDB -> result = "SET @@global.tidb_max_tiflash_threads = " + parallel_degree + " ; ";
            case CommonConfig.DB_POSTGRES ->
                result = "SET max_parallel_workers_per_gather =   " + parallel_degree + " ; ";
            case CommonConfig.DB_POLARDB -> result = "set polar_px_dop_per_node =   " + parallel_degree + " ; ";
            case CommonConfig.DB_OPENGAUSS -> result = "SET query_dop =  " + parallel_degree + " ; ";
            default -> log.error("vodka is not yet compatible with the database");
        }
        return result;
    }

    @Override
    public void run() {
        try {
            this.myThread = Thread.currentThread();
            log.info("AP Workloads Starting");
            executeTPCHWorkload();
            log.info("AP execute end, pass end signal");
            this.conn.close();
        } catch (IOException | SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private static final class QueryTask {
        final int id; // 1–22 对应 tpchSQL/n.sql
        final String path; // "tpchSQL/n.sql"
        final baseQuery q; // 原有 query 对象

        QueryTask(int id, String path, baseQuery q) {
            this.id = id;
            this.path = path;
            this.q = q;
        }
    }

    private void executeTPCHWorkload() throws IOException {
        PreparedStatement stmt, stmtCountCheck, stmtPlan, stmtJsonCheck;
        Statement paralleStmt;
        String[] queryName = new String[queryNumber];
        String[] filterRateLine = new String[queryNumber];
        String[] txnNum = new String[queryNumber];
        double[] latency = new double[queryNumber];
        long[] queryStartTime = new long[queryNumber];
        ArrayList<String> queryPlan = new ArrayList<>();
        ArrayList<String> queryPlanJson = new ArrayList<>();
        boolean recordFileSignal = true;

        try {
            // System.out.println(parallelSwitch);
            if (parallelSwitch) {
                if (dbType == CommonConfig.DB_TIDB || dbType == CommonConfig.DB_POSTGRES) { // set global parallel
                    paralleStmt = conn.createStatement();
                    paralleStmt.execute(parallel_sql);
                    log.info("Executing parallel session sql for global" + parallel_sql);
                    paralleStmt.close();
                }
            }
            if (dbType == CommonConfig.DB_OPENGAUSS) {
                Statement statement1 = conn.createStatement();
                statement1.execute("SET enable_imcsscan=on;");
                System.out.println("Executing Column Store");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        try {
            if (dynamicParam == 1) {
                long startTime = System.nanoTime();
                List<QueryTask> tasks = new ArrayList<>(22);
                // tasks.add(new QueryTask(14, "tpchSQL/14.sql", new Q14(dbType)));
                tasks.add(new QueryTask(1, "tpchSQL/1.sql", new Q1(dbType)));
                // tasks.add(new QueryTask(2, "tpchSQL/2.sql", new Q2(dbType)));
                // tasks.add(new QueryTask(3, "tpchSQL/3.sql", new Q3(dbType)));
                // tasks.add(new QueryTask(4, "tpchSQL/4.sql", new Q4(dbType)));
                // tasks.add(new QueryTask(5, "tpchSQL/5.sql", new Q5(dbType)));
                // tasks.add(new QueryTask(6, "tpchSQL/6.sql", new Q6(dbType)));
                // tasks.add(new QueryTask(7, "tpchSQL/7.sql", new Q7(dbType)));
                // tasks.add(new QueryTask(8, "tpchSQL/8.sql", new Q8(dbType)));
                // tasks.add(new QueryTask(9, "tpchSQL/9.sql", new Q9(dbType)));
                // tasks.add(new QueryTask(10, "tpchSQL/10.sql", new Q10(dbType)));
                // tasks.add(new QueryTask(11, "tpchSQL/11.sql", new Q11(dbType)));
                // tasks.add(new QueryTask(12, "tpchSQL/12.sql", new Q12(dbType)));
                // tasks.add(new QueryTask(13, "tpchSQL/13.sql", new Q13(dbType)));
                // tasks.add(new QueryTask(14, "tpchSQL/14.sql", new Q14(dbType)));
                // tasks.add(new QueryTask(15, "tpchSQL/15.sql", new Q15(dbType)));
                // tasks.add(new QueryTask(16, "tpchSQL/16.sql", new Q16(dbType)));
                // tasks.add(new QueryTask(17, "tpchSQL/17.sql", new Q17(dbType)));
                // tasks.add(new QueryTask(18, "tpchSQL/18.sql", new Q18(dbType)));
                // tasks.add(new QueryTask(19, "tpchSQL/19.sql", new Q19(dbType)));
                // tasks.add(new QueryTask(20, "tpchSQL/20.sql", new Q20(dbType)));
                // tasks.add(new QueryTask(21, "tpchSQL/21.sql", new Q21(dbType)));
                // tasks.add(new QueryTask(22, "tpchSQL/22.sql", new Q22(dbType)));
                queryNumber = tasks.size();

                while (!stopRunningSignal) {
                    long currentLoopTime = System.nanoTime(); // 单位为ns
                    Random seeded = new Random(threadID); // 总是同一条序列
                    if (shuffleQueries) {
                        /* 每轮开始前随机打乱查询顺序 */
                        Collections.shuffle(tasks, seeded); // 结果可重复
                    }
                    if (Double.parseDouble(Long.toString(currentLoopTime - startTime)) / 1_000_000_000 > interval) {
                        recordFileSignal = true;
                        startTime = currentLoopTime;
                    }
                    for (QueryTask task : tasks) {
                        if (stopRunningSignal) {
                            break;
                        }
                        int stage = OLTPClient.CURRENT_STAGE.get(); // 当前阶段
                        int qid = task.id; // 1‒22
                        Path csvPath = stageQFile(stage, qid, "latency", "csv").toPath();
                        Path jsonPath = stageQFile(stage, qid, "plan", "json").toPath();
                        try (SimpleAsyncCsvWriter csvWriter = new SimpleAsyncCsvWriter(csvPath,
                                new String[] { "ts", "latency_ms", "olSize", "ooSize", "olNot", "olRec" });
                                SimpleAsyncCsvWriter jsonWriter = new SimpleAsyncCsvWriter(jsonPath,
                                        new String[] {})) {
                            int idx = qid - 1;
                            queryName[idx] = task.path;
                            stmt = conn.prepareStatement(task.q.updateQuery());
                            String olSize = String.valueOf(orderLineTableSize.intValue());
                            String ooSize = String.valueOf(oorderTableSize.intValue());
                            String olNot = String.valueOf(orderlineTableNotNullSize.intValue());
                            String olRec = String.valueOf(orderlineTableRecipDateNotNullSize.intValue());
                            System.out.println("Executing " + qid);
                            if (detailedPlan) {
                                String explain = "explain (analyze, costs false, timing false, format json) "
                                        + task.q.getQuery();
                                // System.out.println(task.q.getQuery() + "\n");
                                PreparedStatement stm = conn.prepareStatement(explain);
                                long t0wall = System.currentTimeMillis();
                                ResultSet rs = stm.executeQuery();
                                if (rs.next()) {
                                    String planJson = rs.getString(1);
                                    // System.out.println(planJson);
                                    // elapsedMs = extractExecutionTime(planJson);
                                    jsonWriter.writePlan(t0wall, "\"Q" + qid + "\"", task.q.getQuery(), planJson);
                                    csvWriter.writePlanLatency(t0wall, "\"Q" + qid + "\"", planJson,
                                            olSize, ooSize, olNot, olRec);
                                } else {
                                    log.info("Failed to acquire results");
                                }
                            }
                            conn.commit();
                        } catch (Exception ex) {
                            // ex.printStackTrace();
                            continue;
                        }
                    }
                    log.info("Complete a bunch of TPC-H queries");
                    // try {
                    // conn.commit();
                    // } catch (SQLException e) {
                    // e.printStackTrace();
                    // conn.rollback();
                    // }
                }
            }
            log.info("Quit AP threads running");
        } catch (IOException | ParseException | NullPointerException e) {
            e.printStackTrace();
        }
    }

    public void stopRunningWhenPossible() {
        this.stopRunningSignal = true;
        printMessage("Terminal received stop signal!");
        printMessage("Finishing current transaction before exit...");
    }

    /** 取得 “resultDir/stage-<stage>/<prefix>.csv”，不存在就递归创建目录 */
    private File stageFile(String prefix, int stage) throws IOException {
        File dir = new File(resultDirName, "stage-" + stage);
        if (!dir.exists() && !dir.mkdirs())
            throw new IOException("cannot mkdirs " + dir.getAbsolutePath());
        return new File(dir, prefix + "-" + dbType + ".csv");
    }

    private File stageQFile(int stage, int qid, String prefix, String ext) throws IOException {
        File dir = new File(resultDirName, // 根目录
                "stage-" + stage + // 第一层：阶段
                        File.separator + "Q" + qid); // 第二层：查询类别
        if (!dir.exists()) {
            synchronized (this) { // 或者用一个全局锁对象
                if (!dir.exists()) {
                    if (!dir.mkdirs() && !dir.exists()) {
                        throw new IOException("mkdir failed " + dir.getAbsolutePath());
                    }
                }
            }
        }

        return new File(dir, prefix + "-" + dbType + "." + ext); // 保留 dbType
    }

    public void writePlanToFile(ArrayList<String> queryPlan) throws IOException {
        // File file = new File(resultDirName + "/planResult-" + dbType + ".csv"); //
        // 存放数组数据的文件
        File file = stageFile("planResult", OLTPClient.CURRENT_STAGE.get());
        FileWriter out = new FileWriter(file, true);
        for (String s : queryPlan)
            out.write(s);
        out.close();
    }

    public void writePlanJsonToFile(ArrayList<String> queryPlan) throws IOException {
        System.out.println("Write query plan in json layout to file.");
        // File file = new File(resultDirName + "/planJsonResult-" + dbType + ".csv");
        // // 存放数组数据的文件
        File file = stageFile("planJsonResult", OLTPClient.CURRENT_STAGE.get());
        FileWriter out = new FileWriter(file, true);
        out.write(sdf.format(new java.util.Date()) + "\n");
        for (String s : queryPlan)
            out.write(s);
        out.write("orderline table size: " + orderLineTableSize.intValue() + ", order table size: "
                + oorderTableSize.intValue() + ", orderline not null size: " + orderlineTableNotNullSize.intValue()
                + "\n");
        out.close();
    }

    public void writeLineCountCheckToFile(String[] queryName, String[] linesCountCheck) throws IOException {
        System.out.println("Write selectivity to file.");
        // File file = new File(resultDirName + "/lineCountResult-" + dbType + ".csv");
        // // 存放数组数据的文件
        File file = stageFile("lineCountResult", OLTPClient.CURRENT_STAGE.get()); // 存放数组数据的文件
        FileWriter out = new FileWriter(file, true);
        for (int i = 0; i < queryNumber; i++)
            out.write((i + 1) + "," + queryName[i] + "," + linesCountCheck[i] + "," + "countCheck" + "\r");
        out.close();
    }

    private void writeFile(String[] queryName, long[] queryStartTime, double[] latency, String[] txnNum)
            throws IOException {
        log.info("write file");
        // File file = new File(resultDirName + "/tpchresult-" + dbType + ".csv"); //
        // 存放数组数据的文件
        File file = stageFile("tpchresult", OLTPClient.CURRENT_STAGE.get()); // 存放数组数据的文件
        FileWriter out = new FileWriter(file, true); // 文件写入流
        for (int i = 0; i < queryNumber; i++)
            out.write(longToDate(queryStartTime[i]) + "," + (i + 1) + "," + queryName[i] + "," + latency[i] + ","
                    + txnNum[i] + "\r");
        out.close();
    }

    private void printMessage(String message) {
        log.trace(terminalName + ", " + message);
    }

    public static String longToDate(long lo) {
        Date date = new Date(lo);
        SimpleDateFormat sd = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        return sd.format(date);
    }

}

class TxnNumRecord {
    private Connection conn;
    private String getFinishedTxnNumSql = "select new_order, payment from vodka_time;";
    private PreparedStatement stmt = null;
    protected ResultSet result;

    TxnNumRecord(Connection conn, int dbType) {
        this.conn = conn;
        if (CommonConfig.DB_TIDB == dbType)
            getFinishedTxnNumSql = "select /*+ read_from_storage(tiflash[vodka_time]) */ new_order, payment from vodka_time;";
    }

    String getCurrentTxnNum() {
        long new_order = -1;
        long payment = -1;
        try {
            stmt = conn.prepareStatement(getFinishedTxnNumSql);
            result = stmt.executeQuery();
            if (result.next()) {
                new_order = result.getLong(1);
                payment = result.getLong(2);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return new_order + "," + payment;
    }
}

// class SimpleAsyncCsvWriter implements Closeable {
// private final BufferedWriter writer;
// private final ExecutorService exec = Executors.newSingleThreadExecutor();
// private final SimpleDateFormat fmt = new SimpleDateFormat("yyyy-MM-dd
// HH:mm:ss");

// public SimpleAsyncCsvWriter(Path path) throws IOException {
// boolean needHeader = Files.notExists(path) || Files.size(path) == 0;
// writer = Files.newBufferedWriter(path, StandardOpenOption.CREATE,
// StandardOpenOption.APPEND);
// if (needHeader) {
// writer.write("ts,latency_ms,olSize,ooSize,olNot,olRec");
// writer.newLine();
// writer.flush();
// }
// }

// public void log(long timestampMs,
// double latencyMs,
// long olSize, long ooSize,
// long olNot, long olRec) {
// String ts = fmt.format(new Date(timestampMs));
// String line = String.join(",",
// ts,
// Double.toString(latencyMs),
// Long.toString(olSize),
// Long.toString(ooSize),
// Long.toString(olNot),
// Long.toString(olRec));
// exec.submit(() -> {
// try {
// writer.write(line);
// writer.newLine();
// writer.flush();
// } catch (IOException e) {
// e.printStackTrace();
// }
// });
// }

// @Override
// public void close() throws IOException {
// // 停止接收新任务，等待当前任务完成
// exec.shutdown();
// try {
// exec.awaitTermination(5, TimeUnit.SECONDS);
// } catch (InterruptedException e) {
// Thread.currentThread().interrupt();
// }
// writer.close();
// }
// }

class SimpleAsyncCsvWriter implements Closeable {
    private final BufferedWriter writer;
    private final ExecutorService exec = Executors.newSingleThreadExecutor();
    private final SimpleDateFormat fmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

    public SimpleAsyncCsvWriter(Path path, String[] headers) throws IOException {
        boolean needHeader = Files.notExists(path) || Files.size(path) == 0;
        writer = Files.newBufferedWriter(path, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        if (needHeader) {
            writer.write(String.join(",", headers));
            writer.newLine();
            writer.flush();
        }
    }

    /** 普通的 CSV 记录（ts,latency,olSize...） **/
    public void writeCsv(String... columns) {
        String line = String.join(",", columns);
        exec.submit(() -> {
            try {
                writer.write(line);
                writer.newLine();
                writer.flush();
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
    }

    public void writePlan(long timestampMs, String queryLabel, String query, String planJson) {
        exec.submit(() -> {
            try {
                String formatted = formatJsonWithTimestamp(planJson, timestampMs);
                double latMs = extractExecutionTime2(planJson);
                System.out.printf("%s latency is %.3f ms%n", queryLabel, latMs);
                writer.write(formatted);
                writer.newLine(); // 每段 JSON 后再换一行
                writer.flush();
            } catch (Exception e) {
                System.err.println("Error writing plan JSON:");
                e.printStackTrace();
            }
        });
    }

    private double extractExecutionTime2(String jsonStr) {
        Object json = new JSONTokener(jsonStr.trim()).nextValue();

        if (json instanceof JSONObject obj) {
            double execTime = getExecutionTimeFromObject(obj);
            if (execTime >= 0)
                return execTime;
        } else if (json instanceof JSONArray arr) {
            for (int i = 0; i < arr.length(); i++) {
                Object elem = arr.get(i);
                if (elem instanceof JSONObject objElem) {
                    double execTime = getExecutionTimeFromObject(objElem);
                    if (execTime >= 0)
                        return execTime;
                }
            }
        }

        return -1.0;
    }

    private double getExecutionTimeFromObject(JSONObject obj) {
        if (obj.has("Execution Time")) {
            return obj.optDouble("Execution Time", -1.0);
        } else if (obj.has("Total Runtime")) {
            return obj.optDouble("Total Runtime", -1.0);
        }
        return -1.0;
    }

    private String formatJsonWithTimestamp(String jsonStr, long ts) {
        Object json = new JSONTokener(jsonStr.trim()).nextValue();
        if (json instanceof JSONObject obj) {
            obj.put("ts", ts);
            return obj.toString(2);
        } else if (json instanceof JSONArray arr) {
            JSONObject wrapper = new JSONObject();
            wrapper.put("ts", ts);
            wrapper.put("plan_list", arr);
            return wrapper.toString(2);
        } else {
            throw new IllegalArgumentException("Invalid JSON structure: not object or array.");
        }
    }

    public void writePlanLatency(
            long timestampMs,
            String queryTag,
            String planJson,
            String olSize, String ooSize, String olNot, String olRec) {

        exec.submit(() -> {
            try {
                double latencyMs = extractExecutionTime(planJson);
                String ts = fmt.format(new Date(timestampMs));
                String line = String.join(",",
                        ts,
                        String.format(Locale.ROOT, "%.3f", latencyMs),
                        olSize, ooSize, olNot, olRec);

                writer.write(line);
                writer.newLine();
                writer.flush();
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
    }

    @Override
    public void close() throws IOException {
        exec.shutdown();
        try {
            exec.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        writer.close();
    }

    /**
     * 从 EXPLAIN (ANALYZE, FORMAT JSON) 返回的 JSON 里，
     * 查找并解析出 execution time（单位 ms）。
     */
    private double extractExecutionTime(String planJson) {
        // 匹配类似 "Execution Time": 123.456
        Pattern p = Pattern.compile("\"Execution Time\"\\s*:\\s*([0-9]+\\.?[0-9]*)");
        Matcher m = p.matcher(planJson);
        if (m.find()) {
            return Double.parseDouble(m.group(1));
        } else {
            p = Pattern.compile("\"Total Runtime\"\\s*:\\s*([0-9]+\\.?[0-9]*)");
            m = p.matcher(planJson);
            if (m.find()) {
                return Double.parseDouble(m.group(1));
            } else {
                System.out.println("Error");
                return 0.0;
            }

        }
    }
}

// File file1 = new File(resultDirName + "/query.csv"); // 存放数组数据的文件
// FileWriter out1;
// File file2 = new File(resultDirName + "/queryJson.csv"); // 存放数组数据的文件
// FileWriter out2;

// for (int i = 0; i < queryVector.size(); i++) {
// if (stopRunningSignal)
// break;
// queryName[i] = "Vodka-AP-Query-" + (i + 1);
// txnNum[i] = txnNumRecord.getCurrentTxnNum();
// baseQuery obj = queryVector.get(i);

// long startClick, endClick;
// // check query filter rate

// // 在查询前获取统计数据的快照
// int snapshotOrderLineTableSize1 = orderLineTableSize.intValue();
// int snapshotOOrderTableSize1 = oorderTableSize.intValue();
// int snapshotOrderlineNotNullSize1 = orderlineTableNotNullSize.intValue();
// int snapshotOrderlineRecieveNotNullSize1 =
// orderlineTableRecipDateNotNullSize.intValue();
// stmt = conn.prepareStatement(obj.updateQuery());
// int snapshotOrderLineTableSize2 = orderLineTableSize.intValue();
// int snapshotOOrderTableSize2 = oorderTableSize.intValue();
// int snapshotOrderlineNotNullSize2 = orderlineTableNotNullSize.intValue();
// int snapshotOrderlineRecieveNotNullSize2 =
// orderlineTableRecipDateNotNullSize.intValue();

// // 执行查询
// startClick = System.currentTimeMillis();
// ResultSet rs = stmt.executeQuery();
// endClick = System.currentTimeMillis();
// double latency = Double.parseDouble(Long.toString(endClick - startClick));

// queryName[i] = sqlPath[i];
// queryStartTime[i] = startClick;
// latency[i] = latency;

// out1 = new FileWriter(file1, true); // 文件写入流
// out1.write("query " + (i + 1) + ":" + obj.getQuery() + "\r");
// out1.close();

// if (detailedPlan) {
// System.out.println(obj.getQuery());
// stmtJsonCheck = conn.prepareStatement(
// "explain (analyze, costs false, timing false, summary false, format json) "
// + obj.getQuery());
// ResultSet rsPlan = stmtJsonCheck.executeQuery();
// out2 = new FileWriter(file2, true); // 文件写入流
// out2.write(sdf.format(new java.util.Date()) + "\n");
// if (rsPlan.next()) {
// out2.write(queryStartTime[i] + ", " + latency[i]);
// out2.write(queryName[i] + " \n " + rsPlan.getString(1) + "\n");
// }
// out2.write("Before sort: orderline table size: " +
// snapshotOrderLineTableSize1
// + ", order table size: "
// + snapshotOOrderTableSize1 + ", orderline not null size: "
// + snapshotOrderlineNotNullSize1 + ", orderline receivedates not null size: "
// + snapshotOrderlineRecieveNotNullSize1 + "\n");
// out2.write("After sort: orderline table size: " + snapshotOrderLineTableSize2
// + ", order table size: "
// + snapshotOOrderTableSize2 + ", orderline not null size: "
// + snapshotOrderlineNotNullSize2 + ", orderline receivedates not null size: "
// + snapshotOrderlineRecieveNotNullSize2 + "\n");
// out2.close();
// }

// // if (detailedPlan) {
// // System.out.println(obj.getQuery());
// // stmtJsonCheck = conn.prepareStatement(
// // "explain (analyze, costs false, timing false, summary false, format json)
// "
// // + obj.getQuery());
// // ResultSet rsPlan = stmtJsonCheck.executeQuery();
// // if (rsPlan.next()) {
// // queryPlanJson.add(queryName[i] + " \n " + rsPlan.getString(1) + "\n");
// // }
// // }

// if (recordFileSignal)
// log.info(sqlPath[i] + "--" + latency + "ms");
// }

/* ---------- 执行一轮 TPCH 查询 ---------- */
// for (QueryTask task : tasks) {
// if (stopRunningSignal)
// break;

// /* 记录 txn 数等信息 */
// int idx = task.id - 1; // 0-based 索引
// queryName[idx] = task.path; // 原始 sql 路径
// txnNum[idx] = txnNumRecord.getCurrentTxnNum();

// /* 统计快照（查询前） */
// int olSize1 = orderLineTableSize.intValue();
// int ooSize1 = oorderTableSize.intValue();
// int olNot1 = orderlineTableNotNullSize.intValue();
// int olRec1 = orderlineTableRecipDateNotNullSize.intValue();
// stmt = conn.prepareStatement(task.q.updateQuery());

// /* 执行 */
// long start = System.currentTimeMillis();
// ResultSet rs = stmt.executeQuery();
// long end = System.currentTimeMillis();
// double latency = end - start;

// /* 写入数组 */
// queryStartTime[idx] = start;
// latency[idx] = latency;

// /* ——— 结果落盘示例（file1） ——— */
// try (FileWriter out1 = new FileWriter(file1, true)) {
// out1.write("query " + task.id + ":" + task.q.getQuery() + "\r");
// }

// /* 若需计划 */
// if (detailedPlan) {
// try (PreparedStatement ps = conn.prepareStatement(
// "explain (analyze, costs false, timing false, summary false, format json) "
// + task.q.getQuery());
// ResultSet rsPlan = ps.executeQuery();
// FileWriter out2 = new FileWriter(file2, true)) {

// out2.write(sdf.format(new java.util.Date()) + "\n");
// if (rsPlan.next()) {
// out2.write(start + "," + latency + "," + task.path + "\n");
// out2.write(rsPlan.getString(1) + "\n");
// }
// out2.write("Before sort: ol=" + olSize1 + ", oo=" + ooSize1 +
// ", olNot=" + olNot1 + ", olRec=" + olRec1 + "\n");
// }
// }

// if (recordFileSignal)
// log.info(task.path + " -- " + latency + "ms");
// }