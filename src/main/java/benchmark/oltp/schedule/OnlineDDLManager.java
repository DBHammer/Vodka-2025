/* ── benchmark/oltp/schedule/OnlineDDLManager.java ───────────── */
package benchmark.oltp.schedule;

import benchmark.oltp.OLTPClient;
import benchmark.oltp.entity.statement.StmtOnlineDDL;
import config.CommonConfig;
import config.RunningProperty;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public enum OnlineDDLManager {
    INSTANCE;

    /** 独占连接，避免和 TP 线程争锁 */
    private final Connection conn;
    private final StmtOnlineDDL ddl;

    OnlineDDLManager() {
        try {
            String DB_URL = OLTPClient.connectionProperty.getConn();
            String USER = OLTPClient.connectionProperty.getUser();
            String PASS = OLTPClient.connectionProperty.getPassword();
            conn = DriverManager.getConnection(DB_URL, USER, PASS);
            conn.setAutoCommit(true); // 让 StmtOnlineDDL 自己处理 txn
            try (var stmt = conn.createStatement()) {
                stmt.execute("SET max_parallel_workers_per_gather = 128;");
            }
            ddl = new StmtOnlineDDL(conn, CommonConfig.DB_POSTGRES);
        } catch (SQLException e) {
            throw new RuntimeException("Init OnlineDDLManager failed", e);
        }
    }

    public void preRunDDL() throws SQLException{
        ddl.doSplitTable();
        ddl.doPreaggregate();
    }

    /** 供协调器调用——真正执行一条 DDL，阻塞到 commit 完成 */
    void runDDL(int stageIdx) throws SQLException {
        switch (stageIdx) {
            case 2 -> ddl.doAddConstraint();// Add col + constraint
            case 3 -> ddl.doAddColumnWithConstraint(); // Pre-aggregate
            default -> System.out.println("no ddl in this stage");
        }
    }

    // /** 供协调器调用——真正执行一条 DDL，阻塞到 commit 完成 */
    // void runDDL(int stageIdx) throws SQLException {
    //     switch (stageIdx) {
    //         case 0 -> ddl.doCreatePrimaryIndex(); // Create PK
    //         case 1 -> ddl.doAddColumn(); // Add CHECK (ol_number …)
    //         case 2 -> ddl.doAddConstraint();// Add col + constraint
    //         case 3 -> ddl.doAddColumnWithConstraint(); // Pre-aggregate
    //         case 4 -> ddl.doPreaggregate(); // SplitTable
    //         case 5 -> ddl.doSplitTable(); // AddColumn (ol_tax)
    //         default -> throw new IllegalArgumentException(
    //                 "No DDL defined for stage " + stageIdx);
    //     }
    // }
}