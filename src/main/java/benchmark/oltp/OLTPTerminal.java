package benchmark.oltp;

import benchmark.oltp.entity.OLTPData;
import benchmark.synchronize.HTAPCheck;
import benchmark.synchronize.components.TPRecorder;
import benchmark.synchronize.related_work.GlobalTransactionId;
import config.CommonConfig;
import org.apache.log4j.Logger;
import utils.math.random.BasicRandom;

import benchmark.oltp.schedule.*;

import static benchmark.oltp.OLTPClient.isDynamicTest;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;

public class OLTPTerminal implements CommonConfig, Runnable {
    private static Logger log = Logger.getLogger(OLTPTerminal.class);

    private String terminalName;
    private Integer terminalID;
    private Connection conn = null;
    private Statement stmt = null;
    private Statement stmt1 = null;
    private ResultSet rs = null;
    private int terminalWarehouseID, terminalDistrictID;
    private boolean terminalWarehouseFixed;
    private boolean useStoredProcedures;
    private double paymentWeight;
    private double orderStatusWeight;
    private double deliveryWeight;
    private double stockLevelWeight;
    private double receiveGoodsWeight;
    private int limPerMin_Terminal;
    private OLTPClient parent;
    private BasicRandom rnd;
    private int transactionCount = 1;
    private int numTransactions;
    private int numWarehouses;
    private int newOrderCounter;
    private long totalTnxs = 1;
    private StringBuffer query = null;
    private int result = 0;
    private volatile boolean stopRunningSignal = false;
    public static boolean testFreshness = false;
    public static boolean testPerformance = true;

    long terminalStartTime = 0;
    long transactionEnd = 0;

    OLTPConnection db;
    int dbType;
    // htap check variables
    private HTAPCheck htapCheck;
    // tp recorder
    TPRecorder tpRecorder;
    int runningCount = 0;

    // 动态吞吐
    private final ThroughputScheduler scheduler;
    private final int totalTerminals;
    private int segIdx = 0;
    private long timePerTx; // 当前段：每个事务应间隔的毫秒
    private long nextDispatchMillis; // 下一次允许开始事务的绝对时刻
    private long segEndMillis; // 当前段结束的绝对时刻
    private Thread myThread;

    public OLTPTerminal(String terminalName, int terminalWarehouseID, int terminalDistrictID,
            Connection conn, int dbType,
            int numTransactions, boolean terminalWarehouseFixed,
            boolean useStoredProcedures,
            double paymentWeight, double orderStatusWeight,
            double deliveryWeight, double stockLevelWeight, double receiveGoodsWeight,
            int numWarehouses, int limPerMin_Terminal, HTAPCheck htapCheck, TPRecorder tpRecorder, OLTPClient parent)
            throws SQLException {
        this.terminalName = terminalName;
        this.terminalID = Integer.parseInt(terminalName.split("-")[1]); // 获取线程ID
        this.conn = conn;
        this.dbType = dbType;
        this.stmt = conn.createStatement();
        this.stmt.setMaxRows(200);
        this.stmt.setFetchSize(100);
        this.stmt1 = conn.createStatement();
        this.stmt1.setMaxRows(1);
        this.terminalWarehouseID = terminalWarehouseID;
        this.terminalDistrictID = terminalDistrictID;
        this.terminalWarehouseFixed = terminalWarehouseFixed;
        this.useStoredProcedures = useStoredProcedures;
        this.parent = parent;
        this.rnd = parent.getRnd().newRandom();
        this.numTransactions = numTransactions;
        this.paymentWeight = paymentWeight;
        this.orderStatusWeight = orderStatusWeight;
        this.deliveryWeight = deliveryWeight;
        this.stockLevelWeight = stockLevelWeight;
        this.receiveGoodsWeight = receiveGoodsWeight;
        this.numWarehouses = numWarehouses;
        this.newOrderCounter = 0;
        this.limPerMin_Terminal = limPerMin_Terminal;
        this.db = new OLTPConnection(conn, dbType);
        this.htapCheck = htapCheck;
        this.tpRecorder = tpRecorder;
        terminalMessage("");
        terminalMessage("Terminal \'" + terminalName + "\' has WarehouseID=" + terminalWarehouseID + " and DistrictID="
                + terminalDistrictID + ".");
        terminalStartTime = System.currentTimeMillis();
        this.testFreshness = parent.getHtapCheck() != null ? parent.getHtapCheck().info.isHtapCheck : false;
        this.testPerformance = parent.getTerminalsAPstarted() > 0;
        this.scheduler = null;
        this.totalTerminals = 0;
    }

    public OLTPTerminal(String terminalName, int terminalWarehouseID, int terminalDistrictID,
            Connection conn, int dbType,
            int numTransactions, boolean terminalWarehouseFixed,
            boolean useStoredProcedures,
            double paymentWeight, double orderStatusWeight,
            double deliveryWeight, double stockLevelWeight, double receiveGoodsWeight,
            int numWarehouses, int limPerMin_Terminal, HTAPCheck htapCheck, TPRecorder tpRecorder, OLTPClient parent,
            ThroughputScheduler scheduler, int totalTerminals)
            throws SQLException {
        this.terminalName = terminalName;
        this.terminalID = Integer.parseInt(terminalName.split("-")[1]); // 获取线程ID
        this.conn = conn;
        this.dbType = dbType;
        this.stmt = conn.createStatement();
        this.stmt.setMaxRows(200);
        this.stmt.setFetchSize(100);
        this.stmt1 = conn.createStatement();
        this.stmt1.setMaxRows(1);
        this.terminalWarehouseID = terminalWarehouseID;
        this.terminalDistrictID = terminalDistrictID;
        this.terminalWarehouseFixed = terminalWarehouseFixed;
        this.useStoredProcedures = useStoredProcedures;
        this.parent = parent;
        this.rnd = parent.getRnd().newRandom();
        this.numTransactions = numTransactions;
        this.paymentWeight = paymentWeight;
        this.orderStatusWeight = orderStatusWeight;
        this.deliveryWeight = deliveryWeight;
        this.stockLevelWeight = stockLevelWeight;
        this.receiveGoodsWeight = receiveGoodsWeight;
        this.numWarehouses = numWarehouses;
        this.newOrderCounter = 0;
        this.limPerMin_Terminal = limPerMin_Terminal;
        this.db = new OLTPConnection(conn, dbType);
        this.htapCheck = htapCheck;
        this.tpRecorder = tpRecorder;
        terminalMessage("");
        terminalMessage("Terminal \'" + terminalName + "\' has WarehouseID=" + terminalWarehouseID + " and DistrictID="
                + terminalDistrictID + ".");
        terminalStartTime = System.currentTimeMillis();
        this.testFreshness = parent.getHtapCheck() != null ? parent.getHtapCheck().info.isHtapCheck : false;
        this.testPerformance = parent.getTerminalsAPstarted() > 0;
        this.scheduler = scheduler;
        this.totalTerminals = totalTerminals;
    }

    public Thread getThread() {
        return myThread;
    }

    // 动态吞吐
    // private void applySegment(ThroughputScheduler.Segment seg) {
    // ThroughputScheduler.Weights w = ThroughputScheduler.weights(seg.pattern);
    // paymentWeight = w.payment;
    // orderStatusWeight = w.orderStatus;
    // deliveryWeight = w.delivery;
    // stockLevelWeight = w.stockLevel;
    // receiveGoodsWeight = w.receiveGoods;
    // segEndMillis = System.currentTimeMillis() + seg.sec * 1000L;
    // limPerMin_Terminal = seg.tps * 60 / totalTerminals;
    // timePerTx = (limPerMin_Terminal > 0) ? 60000 / limPerMin_Terminal : 0;
    // }

    /** 段切换时：刷新权重 + TPS + 结束时间 + 节拍变量 */
    private void applySegment(ThroughputScheduler.Segment seg) {
        /* 1) 权重 */
        ThroughputScheduler.Weights w = ThroughputScheduler.weights(seg.pattern);
        paymentWeight = w.payment;
        orderStatusWeight = w.orderStatus;
        deliveryWeight = w.delivery;
        stockLevelWeight = w.stockLevel;
        receiveGoodsWeight = w.receiveGoods;

        /* 2) 线程可承载 TPS → timePerTx (毫秒) */
        // limPerMin_Terminal = seg.tps * 60 / totalTerminals;
        // timePerTx = (limPerMin_Terminal > 0) ? 60000 / limPerMin_Terminal : 0;
        // double limPerMin = seg.tps * 60.0 / totalTerminals; // ☆ 不再整除截断
        // timePerTx = (limPerMin > 0) ? // ← 用 double 参与计算
        // (long) Math.ceil(60_000 / limPerMin) // 向上取整，保证不会“踩穿”
        // : 0;
        double tpsPerThread = seg.tps / (double) totalTerminals;
        if (tpsPerThread > 0) {
            timePerTx = (long) Math.ceil(1_000.0 / tpsPerThread);
        } else {
            timePerTx = 0;
        }

        /* 3) 当前段结束时间 */
        segEndMillis = System.currentTimeMillis() + seg.sec * 1000L;
        // System.out.println("SSS" + OLTPClient.isOnlineDDL);
        // System.out.println(segIdx);

        // if (OLTPClient.isOnlineDDL)
        // OnlineDDLCoordinator.ensureStage(segIdx);
        /* 4) “下一拍” 绝对时间戳 —— 让第一条事务立即执行 */
        nextDispatchMillis = System.currentTimeMillis();
        OLTPClient.CURRENT_STAGE.set(segIdx); // ② 每次切段时同步写入
    }

    public void run() {
        try {
            // 动态吞吐首段
            if (scheduler != null && segIdx == 0) {
                applySegment(scheduler.segments().get(0));
            }
            this.myThread = Thread.currentThread();
            SimpleDateFormat simFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            Date startDate = simFormat.parse("1998-08-02 00:00:00");
            Calendar calendar = Calendar.getInstance();
            calendar.setTime(startDate);
            calendar.add(Calendar.SECOND, (int) Math.round(terminalID * OLTPClient.thread_add_interval)); // 按照线程ID，每个线程获取最起始的时间
            Date initialTime = calendar.getTime();
            executeTransactions(numTransactions, initialTime);
        } catch (Exception e) {
            e.printStackTrace();
        } catch (Throwable e) {
            e.printStackTrace();
        }
        try {
            printMessage("");
            printMessage("Closing entity.statement and connection...");
            stmt.close();
            conn.close();
        } catch (Exception e) {
            printMessage("");
            printMessage("An error occurred!");
            logException(e);
            e.printStackTrace();
        }
        printMessage("");
        printMessage("Terminal \'" + terminalName + "\' finished after " + (transactionCount - 1) + " transaction(s).");
        parent.signalTerminalEnded(this, newOrderCounter);
    }

    public void stopRunningWhenPossible() {
        stopRunningSignal = true;
        printMessage("");
        printMessage("Terminal received stop signal!");
        printMessage("Finishing current transaction before exit...");
    }

    private void executeTransactions(int numTransactions, Date initialTime) throws Throwable {
        boolean stopRunning = false;
        if (numTransactions != -1)
            printMessage("Executing " + numTransactions + " entity.transactions...");
        else
            printMessage("Executing for a limited time...");
        // 第一处改动
        // long timePerTx = (limPerMin_Terminal > 0) ? 60000 / limPerMin_Terminal : 0;
        for (int i = 0; (i < numTransactions || numTransactions == -1) && !stopRunning; i++) {
            // 第二处改动
            /* ─── 段切换检测 ───────────────────────────── */
            if (scheduler != null) {
                if (System.currentTimeMillis() >= segEndMillis) {
                    int prevIdx = segIdx; // 记录切换前的编号
                    segIdx++; // 跳到下一段
                    if (segIdx >= scheduler.segments().size())
                        break; // 所有段已跑完
                    applySegment(scheduler.segments().get(segIdx)); // 刷新权重+TPS+节拍
                    nextDispatchMillis = System.currentTimeMillis(); // ← 关键
                    System.out.printf("Thread %s switch %d→%d @ %d%n",
                            terminalName, prevIdx, segIdx, System.currentTimeMillis());
                }
            }
            // 删除原有逻辑
            else {
                timePerTx = (limPerMin_Terminal > 0) ? 60000 / limPerMin_Terminal : 0;
            }
            /* ───────────────────────────────────────────────────── */
            double transactionType = rnd.nextDouble(0.0, 100.0);
            int skippedDeliveries = 0, newOrder = 0;
            String transactionTypeName;
            long transactionStart = System.currentTimeMillis();
            if (OLTPClient.hattrickmode) {
                updateFreshnessTable();
            }
            if (!terminalWarehouseFixed)
                terminalWarehouseID = rnd.nextInt(1, numWarehouses);
            if (transactionType <= paymentWeight) {
                OLTPData term = new OLTPData();
                term.setNumWarehouses(numWarehouses);
                term.setWarehouse(terminalWarehouseID);
                term.setDistrict(terminalDistrictID);
                term.setUseStoredProcedures(useStoredProcedures);
                term.setDBType(dbType);
                term.setTPRecorder(tpRecorder);
                try {
                    term.generatePayment(log, rnd, 0);
                    term.traceScreen(log);
                    term.execute(log, db, rnd, null);
                    parent.resultAppend(term);
                    term.traceScreen(log);
                } catch (Exception e) {
                    log.fatal(e.getMessage());
                    e.printStackTrace();
                    System.exit(1);
                }
                transactionTypeName = "Payment";
                benchmark.oltp.OLTPClient.payment.getAndIncrement();
            } else if (transactionType <= paymentWeight + stockLevelWeight) {
                OLTPData term = new OLTPData();
                term.setNumWarehouses(numWarehouses);
                term.setWarehouse(terminalWarehouseID);
                term.setDistrict(terminalDistrictID);
                term.setUseStoredProcedures(useStoredProcedures);
                term.setDBType(dbType);
                try {
                    term.generateStockLevel(log, rnd, 0);
                    term.traceScreen(log);
                    term.execute(log, db, rnd, null);
                    parent.resultAppend(term);
                    term.traceScreen(log);
                } catch (Exception e) {
                    log.fatal(e.getMessage());
                    e.printStackTrace();
                    System.exit(1);
                }
                transactionTypeName = "Stock-Level";
                benchmark.oltp.OLTPClient.stockLevel.getAndIncrement();
            } else if (transactionType <= paymentWeight + stockLevelWeight + orderStatusWeight) {
                OLTPData term = new OLTPData();
                term.setNumWarehouses(numWarehouses);
                term.setWarehouse(terminalWarehouseID);
                term.setDistrict(terminalDistrictID);
                term.setUseStoredProcedures(useStoredProcedures);
                term.setDBType(dbType);
                try {
                    term.generateOrderStatus(log, rnd, 0);
                    term.traceScreen(log);
                    term.execute(log, db, rnd, null);
                    parent.resultAppend(term);
                    term.traceScreen(log);
                } catch (Exception e) {
                    log.fatal(e.getMessage());
                    e.printStackTrace();
                    System.exit(1);
                }
                transactionTypeName = "Order-Status";
                benchmark.oltp.OLTPClient.orderStatus.getAndIncrement();
            } else if (transactionType <= paymentWeight + stockLevelWeight + orderStatusWeight + receiveGoodsWeight) {
                OLTPData term = new OLTPData();
                term.setNumWarehouses(numWarehouses);
                term.setWarehouse(terminalWarehouseID);
                term.setDistrict(terminalDistrictID);
                try {
                    term.generateReceiveGoods(log, rnd, 0);
                    term.traceScreen(log);
                    if (htapCheck != null)
                        term.setHtapCheck(htapCheck);
                    term.execute(log, db, rnd, null);
                    parent.resultAppend(term);
                    term.traceScreen(log);
                } catch (Exception e) {
                    log.fatal(e.getMessage());
                    e.printStackTrace();
                    System.exit(1);
                }
                transactionTypeName = "Receive-Goods";
                benchmark.oltp.OLTPClient.receiveGoods.getAndIncrement();
            } else if (transactionType <= paymentWeight + stockLevelWeight + orderStatusWeight + receiveGoodsWeight
                    + deliveryWeight) {
                OLTPData term = new OLTPData();
                term.setNumWarehouses(numWarehouses);
                term.setWarehouse(terminalWarehouseID);
                term.setDistrict(terminalDistrictID);
                term.setUseStoredProcedures(useStoredProcedures);
                term.setDBType(dbType);
                try {
                    term.generateDelivery(log, rnd, 0);
                    term.traceScreen(log);
                    term.execute(log, db, rnd, null);
                    parent.resultAppend(term);
                    term.traceScreen(log);
                    OLTPData bg = term.getDeliveryBG();
                    bg.traceScreen(log);
                    if (htapCheck != null) {
                        bg.setHtapCheck(htapCheck);
                    }
                    bg.execute(log, db, rnd, null);
                    parent.resultAppend(bg);
                    bg.traceScreen(log);
                    skippedDeliveries = bg.getSkippedDeliveries();
                } catch (Exception e) {
                    log.fatal(e.getMessage());
                    e.printStackTrace();
                    System.exit(1);
                }
                transactionTypeName = "Delivery";
                benchmark.oltp.OLTPClient.DeliveryBG.getAndIncrement();
            } else {
                OLTPData term = new OLTPData();
                term.setNumWarehouses(numWarehouses);
                term.setWarehouse(terminalWarehouseID);
                term.setDistrict(terminalDistrictID);
                term.setUseStoredProcedures(useStoredProcedures);
                term.setDBType(dbType);
                term.setTPRecorder(tpRecorder);
                try {
                    if (htapCheck != null)
                        term.setHtapCheck(htapCheck);
                    term.generateNewOrder(log, rnd, 0);
                    term.traceScreen(log);
                    Calendar tmpcalendar = Calendar.getInstance();
                    tmpcalendar.setTime(initialTime);
                    tmpcalendar.add(Calendar.SECOND, (int) Math
                            .round(newOrderCounter * OLTPClient.TPterminals.length * OLTPClient.thread_add_interval));
                    Date startTime = tmpcalendar.getTime();
                    if (newOrder % 100 == 0)
                        OLTPClient.currTime = startTime; // 每隔100个事务更新一次全局的OLTPClient.currTime，修正一次deltadays
                    term.execute(log, db, rnd, startTime);
                    parent.resultAppend(term);
                    term.traceScreen(log);
                } catch (Exception e) {
                    log.fatal(e.getMessage());
                    System.out.println("Error happens in Function@OLTPTerminal.class: executeTransactions");
                    e.printStackTrace();
                    System.exit(1);
                }
                transactionTypeName = "New-Order";
                benchmark.oltp.OLTPClient.newOrder.getAndIncrement();
                newOrderCounter++;
                newOrder = 1;
            }
            long transactionEnd = System.currentTimeMillis();
            if (!transactionTypeName.equals("Delivery")) {
                parent.signalTerminalEndedTransaction(this.terminalName, transactionTypeName,
                        transactionEnd - transactionStart, null, newOrder);
            } else {
                parent.signalTerminalEndedTransaction(this.terminalName, transactionTypeName,
                        transactionEnd - transactionStart,
                        (skippedDeliveries == 0 ? "None" : "" + skippedDeliveries + " delivery(ies) skipped."),
                        newOrder);
            }

            /* ---------- 时间戳节拍限速 ---------- */
            /* ---------- 限速（timestamp 节拍） ---------- */
            /* ---- 2) 节拍循环 ---- */
            if (timePerTx > 0) {
                long sleep = nextDispatchMillis - System.currentTimeMillis();
                try {
                    if (sleep > 1) {
                        Thread.sleep(sleep - 1);
                    }
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    break;
                }
                while (System.currentTimeMillis() < nextDispatchMillis)
                    Thread.onSpinWait();
                nextDispatchMillis += timePerTx;
                if (System.currentTimeMillis() > nextDispatchMillis)
                    nextDispatchMillis = System.currentTimeMillis() + timePerTx;
            }
            if (stopRunningSignal)
                stopRunning = true;
        }
    }

    private void logException(Exception e) {
        StringWriter stringWriter = new StringWriter();
        PrintWriter printWriter = new PrintWriter(stringWriter);
        e.printStackTrace(printWriter);
        printWriter.close();
        log.error(stringWriter.toString());
    }

    private void terminalMessage(String message) {
        log.trace(terminalName + ", " + message);
    }

    private void printMessage(String message) {
        log.trace(terminalName + ", " + message);
    }

    private void updateFreshnessTable() {
        int transactionId = GlobalTransactionId.currentId.incrementAndGet(); // 全局事务ID递增
        String updateQuery = "UPDATE freshness_" + terminalID + " SET thread_id = ?, transaction_id = ?";
        OLTPClient.txnIdToCommitTimeMap.put(transactionId, System.currentTimeMillis());
        try (PreparedStatement updateStmt = conn.prepareStatement(updateQuery)) {
            updateStmt.setInt(1, terminalID);
            updateStmt.setInt(2, transactionId);
            int rowsAffected = updateStmt.executeUpdate();
            if (rowsAffected == 0) {
                // 如果没有更新任何行，说明表中没有预先存在的行，需要插入一行
                insertInitialRow();
            }
        } catch (SQLException e) {
            log.error("Error updating Freshness table: " + e.getMessage(), e);
        }
    }

    private void insertInitialRow() {
        String insertQuery = "INSERT INTO freshness_" + terminalID + " (thread_id, transaction_id) VALUES (?, ?)";
        try (PreparedStatement insertStmt = conn.prepareStatement(insertQuery)) {
            insertStmt.setInt(1, terminalID);
            insertStmt.setInt(2, GlobalTransactionId.currentId.get()); // 使用当前事务ID
            insertStmt.executeUpdate();
        } catch (SQLException e) {
            log.error("Error inserting into Freshness table: " + e.getMessage(), e);
        }
    }

}