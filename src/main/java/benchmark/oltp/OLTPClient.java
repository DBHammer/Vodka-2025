package benchmark.oltp;/*
                       * jTPCC - Open Source Java implementation of a TPC-C like benchmark
                       *
                       */

import bean.DBType;
import bean.Order;
import bean.OrderLine;
import bean.ReservoirSampler;
import bean.Triple;
import benchmark.olap.OLAPTerminal;
import benchmark.oltp.entity.OLTPData;
import benchmark.oltp.schedule.ThroughputScheduler;
import benchmark.synchronize.HTAPCheck;
import benchmark.olap.query.*;
import benchmark.synchronize.components.HTAPCheckInfo;
import benchmark.synchronize.components.TPRecorder;
import benchmark.synchronize.related_work.HATtrickFreshness;
import benchmark.synchronize.tasks.TransactionTraceMap;
import config.CommonConfig;
import config.ConnectionProperty;
import config.RunningProperty;
import lombok.Getter;
import org.apache.commons.math3.stat.regression.RegressionResults;
import org.apache.commons.math3.stat.regression.SimpleRegression;
import org.apache.commons.math3.util.Pair;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import utils.common.DynamicAP;
import utils.common.FileUtil;
import utils.common.LongScanTask;
import utils.common.jTPCCUtil;
import utils.math.random.BasicRandom;
import utils.monitor.MemoryMonitor;
import utils.monitor.OSCollector;

import java.io.*;
import java.sql.*;
import java.text.ParseException;
import java.util.Date;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

import static benchmark.olap.OLAPClient.initFilterRatio;

@Getter
public class OLTPClient implements CommonConfig {
    private static org.apache.log4j.Logger log = Logger.getLogger(OLTPClient.class);
    public static int numWarehouses;
    private static String resultDirName = null;
    private static BufferedWriter resultCSV = null;
    private static BufferedWriter runInfoCSV = null;
    private static String propertyPath;
    private static int runID = 0;
    private int dbType = DB_UNKNOWN;
    static OLTPTerminal[] TPterminals;
    private String[] terminalNames;
    private static OLAPTerminal[] APterminals;
    private String[] APterminalNames;
    private boolean terminalsBlockingExit = false;
    private long terminalsStarted = 0, sessionCount = 0;
    public static long transactionCount = 0;
    public static AtomicInteger rollBackTransactionCount = new AtomicInteger(0);
    private long terminalsAPstarted = 0;
    private Object counterLock = new Object();
    private long newOrderCounter = 0, sessionEndTimestamp, sessionNextTimestamp = Long.MAX_VALUE,
            sessionNextKounter = 0;
    public static long sessionStartTimestamp = 0;
    private long sessionEndTargetTime = -1, fastNewOrderCounter;
    public long recentTpmC = 0;
    public static long recentTpmTotal = 0;
    public static double txn_add_interval = 49.5;
    private static boolean signalTerminalsRequestEndSent = false;
    private boolean databaseDriverLoaded;
    private boolean signalAPTerminalsRequestEndSent = false;
    public static String sessionStart, sessionEnd;
    private int limPerMin_Terminal;
    public static AtomicInteger DeliveryBG = new AtomicInteger(0);
    public static AtomicInteger newOrder = new AtomicInteger(0);
    public static AtomicInteger payment = new AtomicInteger(0);
    public static AtomicInteger orderStatus = new AtomicInteger(0);
    public static AtomicInteger stockLevel = new AtomicInteger(0);
    public static AtomicInteger receiveGoods = new AtomicInteger(0);
    public static long lastFreshnessTrigger = System.currentTimeMillis();
    public static long lastDataSharingTrigger = System.currentTimeMillis();
    public static MemoryMonitor memoryMonitor;
    public static LongScanTask longTrx;

    @Getter
    private BasicRandom rnd;
    private OSCollector osCollector = null;
    public static long gloabalSysCurrentTime;
    public final static boolean hattrickmode = false;
    public static HATtrickFreshness haTtrickFreshness;
    public final static Map<Integer, Long> txnIdToCommitTimeMap = new ConcurrentHashMap<>();

    // time density for olap queries
    private int timePointDensity = 10; // 平均每小时点的个数 10
    public static double thread_add_interval = 0; // 一分钟内需要增加的时间间隔
    public static Date currTime = null;
    public static int deltaDays;
    public static int deltaDays2 = 0;
    public static ScheduledExecutorService scheduledExecutorService;
    public static ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    // fitting functions
    public static double b;
    public static double k;
    public static double b1;
    public static double k1;
    public static double b2;
    public static double k2;
    public static List<double[]> generalOorderTSize = new ArrayList<>();
    public static List<double[]> generalOrderlineTSize = new ArrayList<>();
    public static Vector<String> fitFunc1 = new Vector<>(); // oorder
    public static Vector<String> fitFunc2 = new Vector<>(); // order_line
    public static ArrayList<Triple<Long, Long, Long>> freshnessTime;
    public static List<Pair<Long, Long>> freshnessHistory = new ArrayList<>();
    public static ConcurrentLinkedDeque<Pair<Long, Long>> deliveryList = new ConcurrentLinkedDeque<>();
    // public static TransactionTraceMapNew transactionTraceMap = new
    // TransactionTraceMapNew();
    public static TransactionTraceMap transactionTraceMap = new TransactionTraceMap();
    public static ReservoirSampler<OrderLine> sampler4OrderLine = new ReservoirSampler<>(4000000);
    public static ReservoirSampler<Order> sampler4Order = new ReservoirSampler<>(2000000);
    public static ReservoirSampler<Double> sampler4OrderAmt = new ReservoirSampler<>(2000000);
    public static boolean isSampling = false;
    public static boolean isVersionTest = false;
    public static boolean isOnlineDDL = false;
    public static boolean isScaleFilterRatio = false;
    public static boolean isDynamicTest = false;
    public static boolean isSkewDist = false;
    public static boolean isBurst = false;
    public static double scaleFilterRatio = 1;
    public static boolean isFreshness = false;
    public static boolean isDataSharing = false;
    public static boolean isVerifyDS = false;

    // 新增统计指标
    /* ───── Terminal 级成员变量 ───── */
    private long lastPrintMillis = System.currentTimeMillis();
    private long lastTxnTotal = 0; // 上一次打印时的总事务数
    private long lastNewOrderTotal = 0; // 上一次打印时的 C 型事务数
    public static final AtomicInteger CURRENT_STAGE = new AtomicInteger(0); // ① 新增

    // htap check variables
    private HTAPCheckInfo htapCheckInfo = new HTAPCheckInfo();
    private HTAPCheck htapCheck = null;
    public static AtomicInteger htapCheckQueryNumber;

    // TP recorder
    private static TPRecorder tpRecorder = null;

    // properties
    public static ConnectionProperty connectionProperty;
    public static RunningProperty runningProperty;

    // dynamic ap
    private boolean checkInterference;
    private int step;
    private int increaseInterval;

    /* ── 状态文件异步写 ────────────────────────── */
    private static final ExecutorService STATUS_IO = Executors.newSingleThreadExecutor(r -> {
        Thread t = new Thread(r, "status-writer");
        t.setDaemon(true); // JVM 退出时无需等待
        return t;
    });
    private static File statusCsv;
    public static ArrayList<Double> valueSyncList = new ArrayList<Double>();
    public static ArrayList<Double> xidSyncList = new ArrayList<Double>();
    public static ArrayList<Double> xminSyncList = new ArrayList<>();
    private static final Object statusLock = new Object(); // 串行写文件

    public OLTPClient() throws ParseException, IOException {
        loadConfiguration();

        // Connect Parameters
        String iDB = connectionProperty.getDb();
        checkAndSetDBType(iDB);
        String iDriver = connectionProperty.getDriver();
        String iConn = connectionProperty.getConn();
        String iUser = connectionProperty.getUser();
        String iPassword = connectionProperty.getPassword();
        String connAP = connectionProperty.getConnAP();

        // Running Parameters
        numWarehouses = runningProperty.getWarehouses();
        int numTerminals = runningProperty.getTPterminals();
        int iRunTxnsPerTerminal = runningProperty.getRunTxnsPerTerminal();
        double iRunMins = runningProperty.getRunMins();
        double TPthreshold = runningProperty.getTPthreshold();
        double limPerMin = runningProperty.getLimitTxnsPerMin();
        boolean terminalWarehouseFixed = runningProperty.isTerminalWarehouseFixed();
        boolean useStoredProcedures = runningProperty.isUseStoredProcedures();
        double newOrderWeightValue = runningProperty.getNewOrderWeight();
        double paymentWeightValue = runningProperty.getPaymentWeight();
        double orderStatusWeightValue = runningProperty.getOrderStatusWeight();
        double deliveryWeightValue = runningProperty.getDeliveryWeight();
        double stockLevelWeightValue = runningProperty.getStockLevelWeight();
        double receiveGoodsWeightValue = runningProperty.getReceiveGoodsWeight();
        int osCollectorInterval = runningProperty.getOsCollectorInterval();
        String resultDirectory = runningProperty.getResultDirectory();
        String osCollectorScript = runningProperty.getOsCollectorScript();
        String osCollectorDevices = runningProperty.getOsCollectorDevices();
        String osCollectorSSHAddr = runningProperty.getOsCollectorSSHAddr();
        boolean parallelSwitch = runningProperty.isParallel();
        int parallel_degree = runningProperty.getParallel_degree();
        int isolation_level = runningProperty.getIsolation_level();
        boolean iIsHtapCheck = runningProperty.isHtapCheck();
        int testTimeInterval = runningProperty.getTestTimeInterval();
        int APTerminals = runningProperty.getAPTerminals();
        int dynamicParam = runningProperty.getDynamicParam();
        this.isSampling = runningProperty.isSampling();
        this.isVersionTest = runningProperty.isVersionTest();
        this.isDynamicTest = runningProperty.isDynamicTest();
        this.isScaleFilterRatio = runningProperty.isScaleFilterRatio();
        this.isSkewDist = runningProperty.isSkewDist();
        this.isOnlineDDL = runningProperty.isOnlineDDL();
        this.isBurst = runningProperty.isBurst();
        this.scaleFilterRatio = runningProperty.getScaleFilterRatio();
        this.isFreshness = runningProperty.isFreshness();
        this.isDataSharing = runningProperty.isDataSharing();
        this.isVerifyDS = runningProperty.isVerifyDS();

        // dynamic ap
        checkInterference = runningProperty.isCheckInterference();
        step = runningProperty.getStep();
        increaseInterval = runningProperty.getIncreaseInterval();
        htapCheckQueryNumber = new AtomicInteger(runningProperty.getHtapCheckQueryNumber());
        freshnessTime = new ArrayList<>();

        // common variables
        double upLimit = 0.0;
        boolean iRunMinsBool = false;
        this.limPerMin_Terminal = (limPerMin != 0) ? (int) ((limPerMin) * (1 + upLimit) / numTerminals) : -1;
        this.fastNewOrderCounter = 0;
        this.newOrderCounter = 0;
        Properties dbProps = new Properties();
        dbProps.setProperty("user", iUser);
        dbProps.setProperty("password", iPassword);
        TPterminals = new OLTPTerminal[numTerminals];
        terminalNames = new String[numTerminals];
        terminalsStarted = numTerminals;
        int htapCheckCrossTerminalNum = 0;
        APterminals = new OLAPTerminal[APTerminals];
        APterminalNames = new String[APTerminals];
        terminalsAPstarted = APTerminals;

        try {
            printMessage("Loading database driver: \'" + iDriver + "\'...");
            printMessage("WarehouseNumber is: " + numWarehouses);
            printMessage("Target Throughput is: " + limPerMin);
            printMessage("OLTP Thread Number is: " + numTerminals);
            printMessage("OLAP Thread Number is: " + APTerminals);
            printMessage("The target evaluation duration is: " + iRunMins);
            printMessage("isSampling\t" + isSampling);
            printMessage("Isolation Level is: " + isolation_level);
            printMessage("Transaction\tWeight");
            printMessage("% New-Order\t" + newOrderWeightValue);
            printMessage("% Payment\t" + paymentWeightValue);
            printMessage("% Order-Status\t" + orderStatusWeightValue);
            printMessage("% Delivery\t" + deliveryWeightValue);
            printMessage("% Stock-Level\t" + stockLevelWeightValue);
            printMessage("% Receive-Goods\t" + receiveGoodsWeightValue);

            Class.forName(iDriver);
            databaseDriverLoaded = true;
            prepareForLog(resultDirectory, osCollectorScript, osCollectorInterval, osCollectorSSHAddr,
                    osCollectorDevices);
        } catch (Exception ex) {
            errorMessage("Unable to utils.load the database driver!");
            databaseDriverLoaded = false;
        }

        if (databaseDriverLoaded) {
            try {
                boolean limitIsTime = iRunMinsBool;
                int transactionsPerTerminal = -1;
                int loadWarehouses;
                long executionTimeMillis = -1;
                long CLoad;

                try {
                    loadWarehouses = Integer.parseInt(jTPCCUtil.getConfig(iConn, dbProps, "warehouses"));
                    CLoad = Long.parseLong(jTPCCUtil.getConfig(iConn, dbProps, "nURandCLast"));
                    this.rnd = new BasicRandom(CLoad);
                    printMessage("C value for C_LAST during utils.load: " + CLoad);
                    printMessage("C value for C_LAST this run:    " + rnd.getNURandCLast());
                    updateStatusLine();
                } catch (Exception e) {
                    errorMessage(e.getMessage());
                    throw e;
                }

                try {
                    if (iRunMins != 0 && iRunTxnsPerTerminal == 0) {
                        iRunMinsBool = true;
                    } else if (iRunMins == 0 && iRunTxnsPerTerminal != 0) {
                        iRunMinsBool = false;
                    } else {
                        throw new NumberFormatException();
                    }
                } catch (NumberFormatException e1) {
                    errorMessage("Must indicate either entity.transactions per terminal or number of run minutes!");
                    throw new Exception();
                }

                if (numWarehouses > loadWarehouses) {
                    errorMessage("numWarehouses cannot be greater " + "than the warehouses loaded in the database");
                    throw new Exception();
                }
                // try {
                //     if (numTerminals > 10 * numWarehouses)
                //         throw new NumberFormatException();
                // } catch (NumberFormatException e1) {
                //     errorMessage("Invalid number of TPterminals!");
                //     throw new Exception();
                // }
                if (iRunMins != 0 && iRunTxnsPerTerminal == 0) {
                    try {
                        executionTimeMillis = (long) (iRunMins * 60000);
                        if (executionTimeMillis <= 0)
                            throw new NumberFormatException();
                    } catch (NumberFormatException e1) {
                        errorMessage("Invalid number of minutes!");
                        throw new Exception();
                    }
                } else {
                    try {
                        transactionsPerTerminal = iRunTxnsPerTerminal;
                        if (transactionsPerTerminal <= 0)
                            throw new NumberFormatException();
                    } catch (NumberFormatException e1) {
                        errorMessage("Invalid number of entity.transactions per terminal!");
                        throw new Exception();
                    }
                }

                printMessage("Session started!");
                if (!limitIsTime)
                    printMessage("Creating " + numTerminals + " terminal(s) with " + transactionsPerTerminal
                            + " transaction(s) per terminal...");
                else
                    printMessage("Creating " + numTerminals + " terminal(s) with " + (executionTimeMillis / 60000)
                            + " minute(s) of execution...");
                printMessage("Transaction Weights: " + newOrderWeightValue + "% New-Order, " + paymentWeightValue
                        + "% Payment, " + orderStatusWeightValue + "% Order-Status, " + deliveryWeightValue
                        + "% Delivery, " + stockLevelWeightValue + "% Stock-Level");
                printMessage("Number of Terminals\t" + numTerminals);
                printMessage("The data distribution is skew? " + isSkewDist);

                // initialize htap check variables
                htapCheckInfo.isHtapCheck = iIsHtapCheck;
                if (htapCheckInfo.isHtapCheck) {
                    htapCheckInfo.htapCheckType = Integer.parseInt(runningProperty.getHtapCheckType());
                    htapCheckInfo.htapCheckCrossQuantity = Integer
                            .parseInt(runningProperty.getHtapCheckCrossQuantity());
                    htapCheckInfo.htapCheckCrossFrequency = Integer
                            .parseInt(runningProperty.getHtapCheckCrossFrequency());
                    htapCheckInfo.htapCheckApNum = Integer.parseInt(runningProperty.getHtapCheckApNum());
                    htapCheckInfo.gapTime = Double.parseDouble(runningProperty.getHtapCheckGapTime());
                    htapCheckInfo.htapCheckApConn = runningProperty.getHtapCheckConnAp();
                    htapCheckInfo.htapCheckTpConn = iConn;
                    htapCheckInfo.dbType = dbType;
                    htapCheckInfo.resultDir = resultDirName + "/htapcheck";
                    htapCheckInfo.warehouseNum = numWarehouses;
                    htapCheckInfo.htapCheckFreshnessDataBound = runningProperty.getHtapCheckFreshnessDataBound();
                    htapCheckInfo.lFreshLagBound = Integer
                            .parseInt(runningProperty.getHtapCheckFreshLagThreshold().split(",")[0]);
                    htapCheckInfo.rFreshLagBound = Integer
                            .parseInt(runningProperty.getHtapCheckFreshLagThreshold().split(",")[1]);
                    htapCheckInfo.htapCheckQueryNumber = runningProperty.getHtapCheckQueryNumber();
                    htapCheckInfo.isWeakRead = runningProperty.isWeakRead();
                    htapCheckInfo.weakReadTime = runningProperty.getWeakReadTime();

                    htapCheck = new HTAPCheck(htapCheckInfo, dbProps);
                    htapCheckCrossTerminalNum = (int) (0.01f * htapCheckInfo.htapCheckCrossQuantity * numTerminals);
                    switch (htapCheckInfo.dbType) {
                        case CommonConfig.DB_FIREBIRD -> htapCheckInfo.dbTypeStr = "FIREBIRD";
                        case CommonConfig.DB_ORACLE -> htapCheckInfo.dbTypeStr = "ORACLE";
                        case CommonConfig.DB_POSTGRES -> htapCheckInfo.dbTypeStr = "POSTGRES";
                        case CommonConfig.DB_OCEANBASE -> htapCheckInfo.dbTypeStr = "OCEANBASE";
                        case CommonConfig.DB_TIDB -> htapCheckInfo.dbTypeStr = "TIDB";
                        case CommonConfig.DB_POLARDB -> htapCheckInfo.dbTypeStr = "POLARDB";
                        case CommonConfig.DB_OPENGAUSS -> htapCheckInfo.dbTypeStr = "OPENGAUSS";
                    }
                }
                // tpRecorder = new TPRecorder(dbType, iConn, dbProps);
                // tpRecorder = null;

                if (this.dbType == CommonConfig.DB_OPENGAUSS) {
                    System.out.println("initialize column store");
                    initColumnStore(iConn, dbProps);
                }

                // 设置初始时间密度
                if (isVersionTest || APTerminals > 0) {
                    getTableSize(iConn, dbProps);
                    initFilterRatio(iConn, dbProps, dbType, isScaleFilterRatio, scaleFilterRatio);
                    // setTimePointDensity(iConn, dbProps, dbType);
                }

                if (htapCheck != null && hattrickmode) {
                    haTtrickFreshness = new HATtrickFreshness(iConn, connAP, dbProps, numTerminals);
                    haTtrickFreshness.parallelCreateTable();
                }

                if (receiveGoodsWeightValue > 0) {
                    initParametersForReceiveGoods(iConn, dbProps);
                    System.out.println("I am done.");
                }

                if (isFreshness && htapCheck != null) {
                    memoryMonitor = new MemoryMonitor(resultDirName);
                    memoryMonitor.monitor();
                }

                try {
                    int[][] usedTerminals = new int[numWarehouses][10];
                    for (int i = 0; i < numWarehouses; i++)
                        for (int j = 0; j < 10; j++)
                            usedTerminals[i][j] = 0;
                    // 动态吞吐场景控制
                    int totalTerminals = numTerminals;
                    ThroughputScheduler sched = null;
                    if (isDynamicTest) {
                        totalTerminals = numTerminals; // 线程总并发
                        sched = new ThroughputScheduler( /* runMin */ ((int) iRunMins),
                                /* segmentCnt */ 6,
                                /* seed */ 42,
                                /* meanExtra */ 10, isBurst);
                    }
                    boolean ifMultipleThread2Warehouse = numWarehouses != numTerminals;
                    for (int i = 0; i < numTerminals; i++) {
                        int terminalWarehouseID;
                        int terminalDistrictID;
                        do {
                            if (ifMultipleThread2Warehouse)
                                terminalWarehouseID = rnd.nextInt(1, numWarehouses);
                            else
                                terminalWarehouseID = i + 1;
                            terminalDistrictID = rnd.nextInt(1, 10);
                        } while (usedTerminals[terminalWarehouseID - 1][terminalDistrictID - 1] == 1);
                        usedTerminals[terminalWarehouseID - 1][terminalDistrictID - 1] = 1;

                        String terminalName = "Term-" + (i >= 9 ? "" + (i + 1) : "0" + (i + 1));
                        Connection conn;
                        printMessage("Creating database connection for " + terminalName + "...");
                        conn = DriverManager.getConnection(iConn, dbProps);
                        conn.setAutoCommit(false);
                        switch (isolation_level) {
                            case 0 -> conn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
                            case 1 -> conn.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
                            case 2 -> conn.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
                            case 3 -> conn.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
                            default -> conn.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
                        }
                        HTAPCheck curHTAPCheck = null;
                        if (htapCheckCrossTerminalNum > 0) {
                            htapCheckCrossTerminalNum--;
                            curHTAPCheck = htapCheck;
                        }
                        OLTPTerminal terminal = null;
                        if (isDynamicTest) {
                            terminal = new OLTPTerminal(terminalName, terminalWarehouseID, terminalDistrictID,
                                    conn, dbType,
                                    transactionsPerTerminal, terminalWarehouseFixed,
                                    useStoredProcedures,
                                    paymentWeightValue, orderStatusWeightValue,
                                    deliveryWeightValue, stockLevelWeightValue,
                                    receiveGoodsWeightValue, numWarehouses,
                                    limPerMin_Terminal, curHTAPCheck, tpRecorder, this, sched, totalTerminals);
                        } else {
                            terminal = new OLTPTerminal(terminalName, terminalWarehouseID, terminalDistrictID,
                                    conn, dbType,
                                    transactionsPerTerminal, terminalWarehouseFixed,
                                    useStoredProcedures,
                                    paymentWeightValue, orderStatusWeightValue,
                                    deliveryWeightValue, stockLevelWeightValue,
                                    receiveGoodsWeightValue, numWarehouses,
                                    limPerMin_Terminal, curHTAPCheck, tpRecorder, this);
                        }
                        TPterminals[i] = terminal;
                        terminalNames[i] = terminalName;
                        log.trace(terminalName + "\t" + terminalWarehouseID);
                    }

                    sessionEndTargetTime = executionTimeMillis;
                    signalTerminalsRequestEndSent = false;
                    signalAPTerminalsRequestEndSent = false;
                    printMessage("Created " + numTerminals + " terminal(s) successfully!");
                    // table statistic collect thread
                    // 创建Runnable对象并将其作为Thread类的构造函数参数传递
                    // TableInfoCollector infoCollector = new TableInfoCollector(iConn, dbProps,
                    // dbType, (int) limPerMin,
                    // newOrderWeightValue, paymentWeightValue, deliveryWeightValue,
                    // receiveGoodsWeightValue);
                    // Thread thread = new Thread(infoCollector);
                    // thread.start();
                    // System.out.println("Collect Table Info Thread Started: " + thread.getName());

                    if (connAP != null && dbType != CommonConfig.DB_OCEANBASE) {
                        iConn = connAP;
                    }

                    for (int i = 0; i < APTerminals; i++) {
                        String APterminalName = "Term-" + (i >= 9 ? "" + (i + 1) : "0" + (i + 1));
                        OLAPTerminal APTerminal = new OLAPTerminal(iConn, dbProps, dbType, testTimeInterval, this,
                                dynamicParam, parallelSwitch, isolation_level, parallel_degree, resultDirName, i);
                        APterminals[i] = APTerminal;
                        APterminalNames[i] = APterminalName;
                    }

                    // start AP TPterminals
                    if (APTerminals != 0)
                        printMessage("Starting AP terminals...");
                    for (int i = 0; i < APTerminals; i++) {
                        (new Thread(APterminals[i])).start();
                    }

                    // start dynamic ap
                    // if (checkInterference) {
                    // DynamicAP dynamicAP = new DynamicAP(iConn, dbProps, dbType, testTimeInterval,
                    // this,
                    // dynamicParam, parallelSwitch, isolation_level, parallel_degree,
                    // resultDirName, step,
                    // increaseInterval);
                    // Thread dynamicAP_thread = new Thread(dynamicAP);
                    // dynamicAP_thread.start();
                    // }

                    // Create Terminals, Start Transactions
                    sessionStart = getCurrentTime();
                    sessionStartTimestamp = System.currentTimeMillis();
                    sessionNextTimestamp = sessionStartTimestamp;
                    if (sessionEndTargetTime != -1)
                        sessionEndTargetTime += sessionStartTimestamp;
                    // Record run parameters in runInfo.csv
                    if (runInfoCSV != null) {
                        try {
                            StringBuffer infoSB = new StringBuffer();
                            Formatter infoFmt = new Formatter(infoSB);
                            infoFmt.format("%d,simple,%s,%s,%s,%s,%d,%d,%d,%d,%d,1.0,1.0\n", runID, VdokaVersion, iDB,
                                    new Timestamp(sessionStartTimestamp),
                                    iRunMins,
                                    loadWarehouses,
                                    numWarehouses,
                                    numTerminals,
                                    APTerminals,
                                    (int) limPerMin);
                            runInfoCSV.write(infoSB.toString());
                            runInfoCSV.close();
                        } catch (Exception e) {
                            log.error(e.getMessage());
                            System.exit(1);
                        }
                    }
                    synchronized (TPterminals) {
                        printMessage("Starting all TP terminals...");
                        transactionCount = 1;
                        for (OLTPTerminal terminal : TPterminals)
                            (new Thread(terminal)).start();
                    }
                    printMessage("All TPterminals started executing " + sessionStart);
                    log.info("OLTP&OLAP Threads initialize done");
                    // 暂时关闭阈值监测
                    // TPMonitor tpMonitor = new TPMonitor((int) limPerMin, TPthreshold,
                    // APTerminals);
                    // Thread tpMonitor_thread = new Thread(tpMonitor);
                    // tpMonitor_thread.start();

                } catch (Exception e1) {
                    errorMessage("This session ended with errors!");
                    e1.printStackTrace();
                }

            } catch (Exception e2) {
                e2.printStackTrace();
            }
        }
        updateStatusLine();
    }

    public void initColumnStore(String iConn, Properties dbProps) {
        String[] tables = {
                "vodka_config", "vodka_warehouse", "vodka_district", "vodka_customer",
                "vodka_history", "vodka_new_order", "vodka_oorder", "vodka_order_line",
                "vodka_stock", "vodka_item", "vodka_nation", "vodka_region",
                "vodka_supplier", "vodka_time"
        };

        try (Connection connection = DriverManager.getConnection(iConn, dbProps);
                Statement stmt = connection.createStatement()) {

            for (String table : tables) {
                String imcSql = "ALTER TABLE " + table + " IMCSTORED";
                try {
                    stmt.execute(imcSql);
                    System.out.println("Successfully altered: " + table);
                } catch (SQLException e) {
                    String msg = e.getMessage();
                    if (msg != null && msg.contains("has been populated")) {
                        System.err.println("Table already populated, trying UNIMCSTORED + retry: " + table);
                        try {
                            // Step 1: UNIMCSTORED
                            String unImcSql = "ALTER TABLE " + table + " UNIMCSTORED";
                            stmt.execute(unImcSql);
                            System.out.println("Successfully unIMCSTORED: " + table);

                            // Step 2: re-IMCSTORED
                            stmt.execute(imcSql);
                            System.out.println("Successfully re-altered: " + table);
                        } catch (SQLException ex2) {
                            System.err.println("Failed to re-IMCSTORED: " + table);
                            ex2.printStackTrace();
                        }
                    } else {
                        System.err.println("Failed to alter table: " + table);
                        e.printStackTrace();
                    }
                }
            }

        } catch (SQLException e) {
            log.error("Create Column Store Failed", e);
        }
    }

    public void initParametersForReceiveGoods(String iConn, Properties dbProps) {
        final int MAX_WAREHOUSES = 120;
        final int MAX_DISTRICTS_PER_WAREHOUSE = 10;
        final int INITIAL_ORDER_ID = 2100;
        final int TOTAL_ENTRIES = 1000000;

        OLTPData.devOrderIdPerW = new AtomicInteger[TOTAL_ENTRIES];
        OLTPData.recOrderIdPerW = new AtomicInteger[TOTAL_ENTRIES];

        for (int i = 0; i < TOTAL_ENTRIES; i++) {
            OLTPData.devOrderIdPerW[i] = new AtomicInteger(INITIAL_ORDER_ID);
            OLTPData.recOrderIdPerW[i] = new AtomicInteger(INITIAL_ORDER_ID);
        }
        log.info("Initialization of atomic integers completed");
        boolean selftestMode = true;
        if (selftestMode == false) {
            try {
                Connection connection = DriverManager.getConnection(iConn, dbProps);
                String query1 = "SELECT ol_o_id from vodka_order_line where ol_w_id = ? and ol_d_id = ? and ol_delivery_d is not NULL order by ol_o_id DESC";
                String query2 = "SELECT ol_o_id from vodka_order_line where ol_w_id = ? and ol_d_id = ? and ol_receipdate is not NULL order by ol_o_id DESC";

                try (PreparedStatement initStatement1 = connection.prepareStatement(query1);
                        PreparedStatement initStatement2 = connection.prepareStatement(query2)) {

                    for (int w_id = 1; w_id <= MAX_WAREHOUSES; w_id++) {
                        for (int d_id = 1; d_id <= MAX_DISTRICTS_PER_WAREHOUSE; d_id++) {
                            updateAtomicInteger(initStatement1, w_id, d_id, OLTPData.devOrderIdPerW);
                            updateAtomicInteger(initStatement2, w_id, d_id, OLTPData.recOrderIdPerW);
                        }
                    }
                    log.info("Database queries executed and atomic integers updated");
                }
            } catch (SQLException e) {
                log.error("Set table size error");
            }
        }
    }

    private void updateAtomicInteger(PreparedStatement statement, int w_id, int d_id, AtomicInteger[] data)
            throws SQLException {
        statement.setInt(1, w_id);
        statement.setInt(2, d_id);
        try (ResultSet rs = statement.executeQuery()) {
            if (rs.next()) {
                int o_id = rs.getInt(1);
                data[w_id * 10 + d_id].set(o_id);
                log.info("Updated atomic integer for warehouse " + w_id + ", district " + d_id + " to " + o_id);
            }
        }
    }

    public void loadConfiguration() throws IOException {
        // get property file location
        propertyPath = System.getProperty("prop");
        connectionProperty = new ConnectionProperty(propertyPath);
        runningProperty = new RunningProperty(propertyPath);
        connectionProperty.loadProperty();
        runningProperty.loadProperty();
        printStartReport();
    }

    public boolean checkAndSetDBType(String dbName) {
        boolean checkFlag = true;
        switch (dbName) {
            case "oracle" -> dbType = DB_ORACLE;
            case "postgres" -> dbType = DB_POSTGRES;
            case "oceanbase" -> dbType = DB_OCEANBASE;
            case "tidb" -> dbType = DB_TIDB;
            case "polardb" -> dbType = DB_POLARDB;
            case "opengauss" -> dbType = DB_OPENGAUSS;
            default -> {
                checkFlag = false;
                log.error("unknown database type '" + dbName + "'");
            }
        }
        return checkFlag;
    }

    public void prepareForLog(String resultDirectory, String osCollectorScript, int osCollectorInterval,
            String osCollectorSSHAddr, String osCollectorDevices) throws IOException {
        if (databaseDriverLoaded && resultDirectory != null) {
            StringBuffer sb = new StringBuffer();
            Formatter fmt = new Formatter(sb);
            Pattern p = Pattern.compile("%t");
            Calendar cal = Calendar.getInstance();
            String iRunID;
            iRunID = System.getProperty("runID");
            if (iRunID != null)
                runID = Integer.parseInt(iRunID);
            String[] parts = p.split(resultDirectory, -1);
            sb.append(parts[0]);
            for (int i = 1; i < parts.length; i++) {
                fmt.format("%t" + parts[i].charAt(0), cal);
                sb.append(parts[i].substring(1));
            }
            resultDirName = sb.toString();
            File resultDir = new File(resultDirName);
            File resultDataDir = new File(resultDir, "data");
            // Create the output directory structure.
            if (!resultDir.mkdir()) {
                log.error("Failed to create directory '" + resultDir.getPath() + "'");
                System.exit(1);
            }
            if (!resultDataDir.mkdir()) {
                log.error("Failed to create directory '" + resultDataDir.getPath() + "'");
                System.exit(1);
            }
            // Copy the used properties file into the resultDirectory.
            try {
                FileUtil fileUtil = new FileUtil();
                fileUtil.copyFile(new File(System.getProperty("prop")), new File(resultDir, "run.properties"));
            } catch (Exception e) {
                log.error(e.getMessage());
                System.exit(1);
            }
            log.info("Vodka-DBHammer, copied " + System.getProperty("prop") + " to "
                    + new File(resultDir, "run.properties").getPath());
            // Create the runInfo.csv file.
            String runInfoCSVName = new File(resultDataDir, "runInfo.csv").getPath();
            try {
                runInfoCSV = new BufferedWriter(new FileWriter(runInfoCSVName));
                runInfoCSV.write("run,driver,driverVersion,db,sessionStart," + "runMins,"
                        + "loadWarehouses,runWarehouses,numSUTThreads," + "limitTxnsPerMin,"
                        + "thinkTimeMultiplier,keyingTimeMultiplier\n");
            } catch (IOException e) {
                log.error(e.getMessage());
                System.exit(1);
            }
            log.info("Vodka-DBHammer, created " + runInfoCSVName + " for runID " + runID);
            // Open the per transaction result.csv file.
            String resultCSVName = new File(resultDataDir, "result.csv").getPath();
            try {
                resultCSV = new BufferedWriter(new FileWriter(resultCSVName));
                resultCSV.write("run,elapsed,latency,dblatency," + "ttype,rbk,dskipped,error\n");
            } catch (IOException e) {
                log.error(e.getMessage());
                System.exit(1);
            }
            log.info("Vodka-DBHammer, writing per transaction results to " + resultCSVName);
            if (osCollectorScript != null) {
                osCollector = new OSCollector(osCollectorScript, runID, osCollectorInterval,
                        osCollectorSSHAddr,
                        osCollectorDevices, resultDataDir, log);
                osCollector.start();
                log.info("osCollector started.");

            }
        }
    }

    private void printStartReport() {
        log.info("Vodka-DBHammer, ");
        log.info("Vodka-DBHammer, +-------------------------------------------------------------+");
        log.info("Vodka-DBHammer,      BenchmarkSQL v" + VdokaVersion);
        log.info("Vodka-DBHammer, +-------------------------------------------------------------+");
        log.info("Vodka-DBHammer,  (c) 2025, DBHammer@DaSE@ECNU");
        log.info("Vodka-DBHammer, +-------------------------------------------------------------+");
        log.info("Vodka-DBHammer, ");
        log.info("\n" +
                "                   _  _          \n" +
                " /\\   /\\ ___    __| || | __ __ _ \n" +
                " \\ \\ / // _ \\  / _` || |/ // _` |\n" +
                "  \\ V /| (_) || (_| ||   <| (_| |\n" +
                "   \\_/  \\___/  \\__,_||_|\\_\\\\__,_|\n" +
                "                                 \n");
    }

    public static void main(String[] args) throws Exception {
        gloabalSysCurrentTime = System.currentTimeMillis();
        PropertyConfigurator.configure("../run/log4j.properties");
        new OLTPClient();

    }

    public static void signalTerminalsRequestEnd(boolean timeTriggered) {
        synchronized (TPterminals) {
            if (!signalTerminalsRequestEndSent) {
                if (runningProperty.isHtapCheck())
                    scheduler.shutdown();
                if (timeTriggered)
                    printMessage("The time limit has been reached.");
                else
                    printMessage("The TP throughput is too low.");
                printMessage("Signalling all TPterminals to stop...");
                signalTerminalsRequestEndSent = true;
                for (OLTPTerminal terminal : TPterminals)
                    if (terminal != null)
                        terminal.stopRunningWhenPossible();
                printMessage("Waiting for all active entity.transactions to end...");
                for (OLAPTerminal tOlapTerminal : APterminals)
                    if (tOlapTerminal != null)
                        tOlapTerminal.stopRunningWhenPossible();
                printMessage("Waiting for all active ap thread to end...");
                if (memoryMonitor != null)
                    memoryMonitor.stop();
                if (longTrx != null)
                    longTrx.stop();
                try {
                    tpRecorder.close();
                } catch (Throwable e) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    // public static void signalTerminalsRequestEnd(boolean timeTriggered) throws
    // Throwable {
    // synchronized (TPterminals) {
    // if (!signalTerminalsRequestEndSent) {
    // if (runningProperty.isHtapCheck()) {
    // scheduler.shutdownNow();
    // if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
    // log.warn("Scheduler did not shutdown cleanly");
    // }
    // }
    // signalTerminalsRequestEndSent = true;
    // for (OLTPTerminal terminal : TPterminals) {
    // if (terminal != null) {
    // terminal.stopRunningWhenPossible();
    // terminal.getThread().interrupt();
    // }
    // }
    // for (OLTPTerminal terminal : TPterminals)
    // if (terminal != null)
    // terminal.getThread().join(3000);
    // tpRecorder.close();
    // }
    // }

    // synchronized (APterminals) {
    // for (OLAPTerminal terminal : APterminals) {
    // if (terminal != null) {
    // terminal.stopRunningWhenPossible();
    // terminal.getThread().interrupt();
    // }
    // }
    // for (OLAPTerminal terminal : APterminals)
    // if (terminal != null)
    // terminal.getThread().join(3000);
    // }
    // }

    public static void stopBecauseOfLowTP() throws Throwable {
        signalTerminalsRequestEnd(false);
    }

    public void signalTerminalEnded(OLTPTerminal terminal, long countNewOrdersExecuted) {
        synchronized (TPterminals) {
            boolean found = false;
            terminalsStarted--;
            for (int i = 0; i < TPterminals.length && !found; i++) {
                if (TPterminals[i] == terminal) {
                    TPterminals[i] = null;
                    terminalNames[i] = "(" + terminalNames[i] + ")";
                    newOrderCounter += countNewOrdersExecuted;
                    found = true;
                }
            }
        }
        if (terminalsStarted == 0) {
            sessionEnd = getCurrentTime();
            sessionEndTimestamp = System.currentTimeMillis();
            sessionEndTargetTime = -1;
            printMessage("All TPterminals finished executing " + sessionEnd);
            endReport();
            terminalsBlockingExit = false;
            printMessage("Session finished!");

            // If we opened a per transaction result file, close it.
            if (resultCSV != null) {
                try {
                    resultCSV.close();
                } catch (IOException e) {
                    log.error(e.getMessage());
                }
            }

            // Stop the OSCollector, if it is active.
            if (osCollector != null) {
                osCollector.stop();
                osCollector = null;
            }
        }
    }

    public void signalTerminalEndedTransaction(String terminalName, String transactionType, long executionTime,
            String comment, int newOrder) throws Throwable {
        synchronized (counterLock) {
            transactionCount++;
            fastNewOrderCounter += newOrder;
        }
        if (sessionEndTargetTime != -1 && System.currentTimeMillis() > sessionEndTargetTime) {
            signalTerminalsRequestEnd(true);
            // signalAPTerminalsRequestEnd(true);
        }
        updateStatusLine();

    }

    public void resultAppend(OLTPData term) {
        if (resultCSV != null) {
            try {
                resultCSV.write(runID + "," +
                        term.resultLine(sessionStartTimestamp));
            } catch (IOException e) {
                log.error("Vodka-DBHammer, " + e.getMessage());
            }
        }
    }

    private void endReport() {
        long currTimeMillis = System.currentTimeMillis();
        long freeMem = Runtime.getRuntime().freeMemory() / (1024 * 1024);
        long totalMem = Runtime.getRuntime().totalMemory() / (1024 * 1024);
        double tpmC = (6000000 * fastNewOrderCounter / (currTimeMillis - sessionStartTimestamp + 0.00001)) / 100.0;
        double tpmTotal = (6000000 * transactionCount / (currTimeMillis - sessionStartTimestamp + 0.00001)) / 100.0;

        System.out.println();
        log.info("Vodka-DBHammer, ");
        log.info("Vodka-DBHammer, ");
        log.info("Vodka-DBHammer, Measured tpmC (NewOrders) = " + tpmC);
        log.info("Vodka-DBHammer, Measured tpmTOTAL = " + tpmTotal);
        log.info("Vodka-DBHammer, Session Start     = " + sessionStart);
        log.info("Vodka-DBHammer, Session End       = " + sessionEnd);
        log.info("Vodka-DBHammer, Transaction Count = " + (transactionCount - 1));

        log.info("benchmark.oltp.OLTPClient.transactionCount:" + benchmark.oltp.OLTPClient.transactionCount);
        log.info("benchmark.oltp.OLTPClient.DeliveryBG:" + benchmark.oltp.OLTPClient.DeliveryBG.get());
        log.info("benchmark.oltp.OLTPClient.newOrder:" + benchmark.oltp.OLTPClient.newOrder.get());
        log.info("benchmark.oltp.OLTPClient.orderStatus:" + benchmark.oltp.OLTPClient.orderStatus.get());
        log.info("benchmark.oltp.OLTPClient.payment:" + benchmark.oltp.OLTPClient.payment.get());
        log.info("benchmark.oltp.OLTPClient.receiveGoods:" + benchmark.oltp.OLTPClient.receiveGoods.get());
        log.info("benchmark.oltp.OLTPClient.stockLevel:" + benchmark.oltp.OLTPClient.stockLevel.get());
        log.info("benchmark.oltp.OLTPClient.rollBackTransactionCount"
                + benchmark.oltp.OLTPClient.rollBackTransactionCount.get());
        log.info("OLAPTerminal.oorderTableSize:" + OLAPTerminal.oorderTableSize.get());
        log.info("OLAPTerminal.orderLineTableSize:" + OLAPTerminal.orderLineTableSize.get());
        log.info("OLAPTerminal.orderlineTableNotNullSize:" + OLAPTerminal.orderlineTableNotNullSize.get());
        log.info("OLAPTerminal.orderlineTableRecipDateNotNullSize:"
                + OLAPTerminal.orderlineTableRecipDateNotNullSize.get());
        STATUS_IO.shutdown();

    }

    private static void printMessage(String message) {
        log.info("Vodka-DBHammer, " + message);
    }

    private void errorMessage(String message) {
        log.error("Vodka-DBHammer, " + message);
    }

    private void exit() {
        System.exit(0);
    }

    private String getCurrentTime() {
        return dateFormat.format(new java.util.Date());
    }

    synchronized private void updateStatusLine() {
        statusCsv = new File(resultDirName, "tps.csv");
        long now = System.currentTimeMillis();
        long msElapsed = now - lastPrintMillis; // 距上次打印的毫秒数
        if (msElapsed < 1000)
            return; // 控制 1 Hz 输出

        /* ① 计算瞬时值（最近 1 s） */
        long txnDelta = transactionCount - lastTxnTotal;
        long newOrdDelta = fastNewOrderCounter - lastNewOrderTotal;
        double instTPS = txnDelta * 1000.0 / msElapsed;
        double instCPS = newOrdDelta * 1000.0 / msElapsed;

        /* ② 计算全局平均（自会话开始） */
        long runMs = now - sessionStartTimestamp;
        double avgTPM = transactionCount * 60000.0 / runMs;
        double avgCPM = fastNewOrderCounter * 60000.0 / runMs;

        /* ③ 打印到控制台 */
        System.out.printf(
                "progress: %.1f s | tpmTOTAL: %.1f | tpmC: %.1f | instTPS: %.0f | instCPS: %.0f%n",
                runMs / 1000.0, avgTPM, avgCPM, instTPS, instCPS);

        /* ④ 把同样的数据丢给后台线程写 CSV（异步，非阻塞） */
        final String csvLine = String.format("%d,%.0f,%.0f,%.0f,%.0f%n",
                now, avgTPM, avgCPM, instTPS, instCPS);

        STATUS_IO.submit(() -> {
            try {
                synchronized (statusLock) { // 串行写入同一个文件
                    boolean needHeader = (statusCsv == null) ||
                            (!statusCsv.exists()) ||
                            (statusCsv.length() == 0);

                    try (FileWriter w = new FileWriter(statusCsv, true)) {
                        if (needHeader) {
                            w.write("ts_ms,avgTPM,avgCPM,instTPS,instCPS\n");
                        }
                        w.write(csvLine);
                    }
                }
            } catch (IOException ioe) {
                // 这里只做吞吐日志，出错时打印警告即可
                System.err.println("status CSV write failed: " + ioe.getMessage());
            }
        });

        /* ⑤ 更新基准点 */
        lastPrintMillis = now;
        lastTxnTotal = transactionCount;
        lastNewOrderTotal = fastNewOrderCounter;
    }

    // synchronized private void updateStatusLine() {
    // if (isDynamicTest == false) {
    // long currTimeMillis = System.currentTimeMillis();
    // if (currTimeMillis > sessionNextTimestamp) {
    // StringBuilder informativeText = new StringBuilder();
    // Formatter fmt = new Formatter(informativeText);
    // double tpmC = (6000000 * fastNewOrderCounter / (currTimeMillis -
    // sessionStartTimestamp + 0.00001))
    // / 100.0;
    // double tpmTotal = (6000000 * transactionCount / (currTimeMillis -
    // sessionStartTimestamp + 0.00001))
    // / 100.0;
    // sessionNextTimestamp += 1000; /* update this every seconds */
    // fmt.format("progress: %.1f, tpmTOTAL: %.1f, tpmC: %.1f",
    // (double) (currTimeMillis - sessionStartTimestamp) / 1000, tpmTotal, tpmC);
    // recentTpmTotal = (transactionCount - sessionNextKounter) * 12;
    // recentTpmC = (fastNewOrderCounter - sessionNextKounter) * 12;
    // sessionNextKounter = fastNewOrderCounter;
    // System.out.println(informativeText);
    // }
    // } else {
    // long now = System.currentTimeMillis();
    // if (now > sessionNextTimestamp) {
    // /* --- ① 计算 5 秒窗口 --- */
    // long winLenMs = now - winStartTs; // 实际窗口长度
    // long txnDelta = transactionCount - winTxnCount;
    // long newOrderDelta = fastNewOrderCounter - winNewOrdCnt;
    // double tpsWinTotal = (winLenMs > 0) ? txnDelta * 1000.0 / winLenMs : 0;
    // double tpsWinC = (winLenMs > 0) ? newOrderDelta * 1000.0 / winLenMs : 0;
    // /* --- ② 打印 --- */
    // System.out.printf("elapsed %.0fs | instTPS %.0f | instC %.0f%n",
    // (now - sessionStartTimestamp) / 1000.0, tpsWinTotal, tpsWinC);
    // /* --- ③ 每 5 s 滑动窗口起点 --- */
    // if (winLenMs >= 5000) { // >5s 时重置
    // winStartTs = now;
    // winTxnCount = transactionCount;
    // winNewOrdCnt = fastNewOrderCounter;
    // }
    // sessionNextTimestamp = now + 1000; // 1 Hz 输出
    // }
    // }

    // }

    public static double collectTpmC() {
        return (6000000 * transactionCount / (System.currentTimeMillis() - sessionStartTimestamp + 0.00001)) / 100.0;
    }

    public static boolean getSignalTerminalsRequestEndSent() {
        return signalTerminalsRequestEndSent;
    }

    public void getTableSize(String iConn, Properties dbProps) {
        log.info("Getting Table Size...");
        try {
            Connection connection = DriverManager.getConnection(iConn, dbProps);
            Statement statement = connection.createStatement();
            statement.execute("SET query_dop = 64;");
            // statement.execute("SET enable_imcsscan=on;");
            ResultSet rs = statement.executeQuery("select count(*) from vodka_oorder;");
            if (rs.next())
                baseQuery.orderOriginSize = rs.getInt(1);
            rs = statement.executeQuery("select count(*) from vodka_order_line;");
            if (rs.next())
                baseQuery.olOriginSize = rs.getInt(1);
            rs = statement.executeQuery("select count(*) from vodka_order_line where ol_delivery_d is not null;");
            if (rs.next())
                baseQuery.olNotnullSize = rs.getInt(1);
            rs.close();
        } catch (SQLException e) {
            log.error("Set table size error");
        }
    }

    public void setTimePointDensity(String iConn, Properties dbProps, int dbType) {
        log.info("Acquiring time density information...");
        try {
            Connection connection = DriverManager.getConnection(iConn, dbProps);
            String sql;
            switch (dbType) {
                case (CommonConfig.DB_TIDB) ->
                    sql = "select avg(counts) from (select year(o_entry_d),month(o_entry_d),day(o_entry_d),hour(o_entry_d),count(*) as counts "
                            +
                            " from vodka_oorder group by year(o_entry_d),month(o_entry_d),day(o_entry_d),hour(o_entry_d)) as L;";
                case (CommonConfig.DB_OCEANBASE) ->
                    sql = "select avg(counts) from (select year(o_entry_d),month(o_entry_d),day(o_entry_d),hour(o_entry_d),count(*) as counts "
                            +
                            " from vodka_oorder group by year(o_entry_d),month(o_entry_d),day(o_entry_d),hour(o_entry_d));";
                default -> sql = "SELECT AVG(counts) " +
                        "FROM ( " +
                        "    SELECT " +
                        "        EXTRACT(YEAR FROM o_entry_d) AS year, " +
                        "        EXTRACT(MONTH FROM o_entry_d) AS month, " +
                        "        EXTRACT(DAY FROM o_entry_d) AS day, " +
                        "        EXTRACT(HOUR FROM o_entry_d) AS hour, " +
                        "        COUNT(*) AS counts " +
                        "    FROM vodka_oorder " +
                        "    GROUP BY " +
                        "        EXTRACT(YEAR FROM o_entry_d), " +
                        "        EXTRACT(MONTH FROM o_entry_d), " +
                        "        EXTRACT(DAY FROM o_entry_d), " +
                        "        EXTRACT(HOUR FROM o_entry_d) " +
                        ") AS L;";
            }
            Statement statement = connection.createStatement();
            ResultSet rs = statement.executeQuery(sql);
            if (rs.next())
                this.timePointDensity = rs.getInt(1);
            rs.close();
            statement.close();
            connection.close();
            thread_add_interval = 3600.0 / timePointDensity;
            log.info("Current Average Time Density is: " + timePointDensity);
            log.info("Time Interval Per Thread is: " + thread_add_interval);
        } catch (SQLException e) {
            log.error("Set time density error");
        }
    }

    public class LinerFitResult {
        public double b;
        public double k;
        public double w1;
        public double w2;
        public double b2;

        public LinerFitResult(double ttb, double ttk) {
            this.b = ttb;
            this.k = ttk;
        }

        public LinerFitResult(double w1, double w2, double b2) {
            this.w1 = w1;
            this.w2 = w2;
            this.b2 = b2;
        }
    }

    public LinerFitResult linearFit(double[][] data, int degree) {
        if (data != null) {
            if (degree == 1) {
                List<double[]> fitData = new ArrayList<>(); // 这个fitData有啥用, 没有被访问的话，底下的for-loop不是也没用嘛
                SimpleRegression regression = new SimpleRegression();
                regression.addData(data); // 数据集
                RegressionResults results = regression.regress();
                double tb = results.getParameterEstimate(0);
                double tk = results.getParameterEstimate(1);
                // 重新计算生成拟合曲线
                for (double[] datum : data) {
                    double[] xy = { datum[0], tk * datum[0] + tb };
                    fitData.add(xy);
                }
                return new LinerFitResult(tb, tk);
            } else {
                return null;
            }
        } else {
            return null;
        }
    }

    synchronized public static double[][] linearScatters(List<double[]> tdata) {
        if (tdata.size() > 5) {
            if (tdata.size() > 30)
                tdata = tdata.subList(tdata.size() - 30, tdata.size());
            return tdata.toArray(new double[0][0]);
        } else
            return null;
    }

}