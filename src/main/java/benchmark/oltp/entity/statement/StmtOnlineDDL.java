package benchmark.oltp.entity.statement;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import bean.Order;
import bean.ReservoirSampler;
import benchmark.oltp.OLTPClient;

/**
 * Online-DDL statements executed as single-statement txns.
 * 每个 doXxx() 都会独立启/提/回事务，与微基准保持一致。
 */
public class StmtOnlineDDL extends StmtBasic {

    /* ① PreparedStatements —— 所有 DDL */
    private final Map<PreparedStatement, String> ddlSqlMap = new HashMap<>();
    public final PreparedStatement stmtAddColumn; // AddColumn
    public final PreparedStatement stmtAddConstraint; // AddConstraint
    public final PreparedStatement stmtAddColumnWithConstraint; // AddColumn+Constraint
    public final PreparedStatement stmtSplitTableCreatePrivate; // SplitTable-private
    public final PreparedStatement stmtSplitTableCreatePublic; // SplitTable-public
    public final PreparedStatement stmtPreAggAddCol; // Preagg-add-column
    public final PreparedStatement stmtPreAggUpdate; // Preagg-step-B：批量 UPDATE
    public final PreparedStatement stmtCreatePrimaryIndex; // CreateIndex

    /* ② 构造函数 —— 准备所有语句 */
    public StmtOnlineDDL(Connection dbConn, int dbType) throws SQLException {

        /* ---------- baseline（PostgreSQL 语法，TiDB 也兼容） ---------- */
        String addColumn = """
                ALTER TABLE vodka_order_line
                ADD COLUMN IF NOT EXISTS ol_tax DECIMAL(5,2) DEFAULT 0.10
                """;

        String addConstraint = """
                ALTER TABLE vodka_order_line
                ADD CONSTRAINT chk_ol_number
                CHECK (ol_number BETWEEN 1 AND 15)
                """;

        String addColumnWithConstraint = """
                ALTER TABLE vodka_order_line
                ADD COLUMN IF NOT EXISTS ol_tax DECIMAL(5,2) DEFAULT 0.10,
                ADD CONSTRAINT chk_ol_amount CHECK (ol_amount >= 0)
                """;

        /* —— SplitTable: 按隐私/公开拆 customer —— */
        String splitPrivate = """
                CREATE TABLE IF NOT EXISTS vodka_customer_private AS
                SELECT c_id,c_w_id,c_d_id,
                       c_credit,c_payment_cnt,c_balance, c_phone
                  FROM vodka_customer
                """;

        String splitPublic = """
                CREATE TABLE IF NOT EXISTS vodka_customer_public AS
                SELECT c_id,c_w_id,c_d_id,
                c_nationkey,c_city,c_mktsegment,
                       c_street_1,c_street_2,c_zip
                  FROM vodka_customer
                """;

        /* —— Preaggregate：两步法 —— */
        String preAggAddCol = """
                ALTER TABLE vodka_oorder
                ADD COLUMN IF NOT EXISTS ol_amount_sum DECIMAL(12,2) DEFAULT 0
                """;

        // step-A：把 order_line 聚合结果写入临时表（并方便 Java 侧做蓄水池采样）
        String preAggUpdate = """
                WITH agg AS (
                    SELECT ol_w_id, ol_d_id, ol_o_id,
                           SUM(ol_amount) AS amt
                      FROM vodka_order_line
                     GROUP BY ol_w_id, ol_d_id, ol_o_id
                )
                UPDATE vodka_oorder AS o
                   SET ol_amount_sum = a.amt
                  FROM agg AS a
                 WHERE a.ol_w_id = o.o_w_id
                   AND a.ol_d_id = o.o_d_id
                   AND a.ol_o_id = o.o_id
                """;

        /* —— CreateIndex：加主键 —— */
        String createPrimaryIndex = """
                ALTER TABLE vodka_order_line
                ADD CONSTRAINT order_line_pkey
                PRIMARY KEY (ol_w_id, ol_d_id, ol_o_id, ol_number)
                """;

        /* ---------- per-DBMS override（示例预留） ---------- */
        switch (dbType) {
            // case CommonConfig.DB_MYSQL: ...; break;
            default -> {
                /* 使用 baseline */ }
        }

        /* ---------- prepare ---------- */
        stmtAddColumn = dbConn.prepareStatement(addColumn);
        stmtAddConstraint = dbConn.prepareStatement(addConstraint);
        stmtAddColumnWithConstraint = dbConn.prepareStatement(addColumnWithConstraint);

        stmtSplitTableCreatePrivate = dbConn.prepareStatement(splitPrivate);
        stmtSplitTableCreatePublic = dbConn.prepareStatement(splitPublic);

        stmtPreAggAddCol = dbConn.prepareStatement(preAggAddCol);
        stmtPreAggUpdate = dbConn.prepareStatement(preAggUpdate);

        stmtCreatePrimaryIndex = dbConn.prepareStatement(createPrimaryIndex);

        ddlSqlMap.put(stmtAddColumn, addColumn);
        ddlSqlMap.put(stmtAddConstraint, addConstraint);
        ddlSqlMap.put(stmtAddColumnWithConstraint, addColumnWithConstraint);
        ddlSqlMap.put(stmtSplitTableCreatePrivate, splitPrivate);
        ddlSqlMap.put(stmtSplitTableCreatePublic, splitPublic);
        ddlSqlMap.put(stmtPreAggAddCol, preAggAddCol);
        ddlSqlMap.put(stmtPreAggUpdate, preAggUpdate);
        ddlSqlMap.put(stmtCreatePrimaryIndex, createPrimaryIndex);
    }

    /* ③ 封装：每次一个事务、一次 DDL */
    public void doAddColumn() throws SQLException {
        execSingleDDL(stmtAddColumn);
    }

    public void doAddConstraint() throws SQLException {
        execSingleDDL(stmtAddConstraint);
    }

    public void doAddColumnWithConstraint() throws SQLException {
        execSingleDDL(stmtAddColumnWithConstraint);
    }

    public void doSplitTable() throws SQLException {
        execSingleDDL(stmtSplitTableCreatePrivate);
        execSingleDDL(stmtSplitTableCreatePublic); // 两条独立事务
    }

    // public void doPreaggregate() throws SQLException {
    // Connection c = stmtPreAggUpdate.getConnection();
    // boolean prevAuto = c.getAutoCommit();
    // try {
    // c.setAutoCommit(false);

    // // // A) 添加列
    // // stmtPreAggAddCol.execute();

    // // // B) 执行 UPDATE（预聚合）
    // // System.out.println("[PREAGG] Running bulk update of ol_amount_sum …");
    // // int updated = stmtPreAggUpdate.executeUpdate();
    // // System.out.println("[PREAGG] UPDATE affected " + updated + " orders.");

    // // C) 全表扫描两个字段进行联合采样
    // System.out.println("[PREAGG] Scanning o_entry_d and ol_amount_sum for
    // sampling …");
    // try (Statement st = c.createStatement();
    // ResultSet rs = st.executeQuery("SELECT o_entry_d, ol_amount_sum FROM
    // vodka_oorder")) {

    // long count = 0;
    // final long logInterval = 100_000;

    // while (rs.next()) {
    // Timestamp entry = rs.getTimestamp("o_entry_d");
    // double amount = rs.getDouble("ol_amount_sum");
    // Order order = new Order(entry, amount);

    // // 联合采样
    // OLTPClient.sampler4Order.add(order);
    // // OLTPClient.sampler4OrderAmt.add(amount); // 如你仍需要保留单变量采样
    // if (++count % logInterval == 0) {
    // System.out.println("[PREAGG] Sampled rows: " + count);
    // }
    // }
    // System.out.println("[PREAGG] Sampling complete, total rows: " + count);
    // }

    // // D) 提交
    // c.commit();
    // OLTPClient.sampler4Order.sortForInitialize();
    // OLTPClient.sampler4OrderAmt.sortForInitialize();
    // System.out.println("[PREAGG] Pre-aggregation committed.");
    // } catch (SQLException e) {
    // c.rollback();
    // System.err.println("[PREAGG] ERROR, rolled back: " + e.getMessage());
    // throw e;
    // } finally {
    // c.setAutoCommit(prevAuto);
    // }
    // }

    // 2) doPreaggregate 方法改为：先跑 UPDATE，再全表扫描 ol_amount_sum 列进行采样
    public void doPreaggregate() throws SQLException {
        Connection c = stmtPreAggUpdate.getConnection();
        boolean prevAuto = c.getAutoCommit();
        try {
            c.setAutoCommit(false);
            // A) 先加列
            stmtPreAggAddCol.execute();

            // // B) 执行 UPDATE（不带 RETURNING）
            // System.out.println("[PREAGG] Running bulk update of ol_amount_sum …");
            // int updated = stmtPreAggUpdate.executeUpdate();
            // System.out.println("[PREAGG] UPDATE affected " + updated + " orders.");

            // C) 全表扫描 ol_amount_sum 列进行采样
            System.out.println("[PREAGG] Scanning ol_amount_sum for sampling …");
            try (Statement st = c.createStatement();
                    ResultSet rs = st.executeQuery("SELECT ol_amount_sum FROM vodka_oorder")) {
                long count = 0;
                final long logInterval = 100_000;
                while (rs.next()) {
                    OLTPClient.sampler4OrderAmt.add(rs.getDouble(1));
                    if (++count % logInterval == 0) {
                        System.out.println("[PREAGG] Sampled “ol_amount_sum” rows: " + count);
                    }
                }
                System.out.println("[PREAGG] Sampling complete, total rows: " + count);
            }

            // D) 提交事务
            c.commit();
            OLTPClient.sampler4OrderAmt.sortForInitialize();
            System.out.println("[PREAGG] Pre-aggregation committed.");
        } catch (SQLException e) {
            c.rollback();
            System.err.println("[PREAGG] ERROR, rolled back: " + e.getMessage());
            throw e;
        } finally {
            c.setAutoCommit(prevAuto);
        }
    }

    public void doCreatePrimaryIndex() throws SQLException {
        try {
            execSingleDDL(stmtCreatePrimaryIndex);
        } catch (SQLException e) {
            if (e.getMessage().contains("already exists")) {
                System.out.println("[DDL] 主键约束已存在，跳过");
            } else {
                throw e;
            }
        }
    }

    /* ---------- 通用执行器 ---------- */
    private void execSingleDDL(PreparedStatement ps) throws SQLException {
        Connection c = ps.getConnection();
        boolean prevAuto = c.getAutoCommit();
        String sqlText = ddlSqlMap.getOrDefault(ps, "[Unknown SQL]");
        System.out.println("[Executing DDL] >>> " + sqlText.trim());
        try {
            c.setAutoCommit(false);
            ps.executeUpdate();
            c.commit();
        } catch (SQLException e) {
            c.rollback();
            throw e;
        } finally {
            c.setAutoCommit(prevAuto);
        }
    }

    public static void main(String[] args) throws Exception {
        String DB_URL = "jdbc:postgresql://49.52.27.35:5532/benchmarksql";
        String USER = "postgres";
        String PASS = "";
        // 1) 确保 OnlineDDL 模式打开，这样 sampler4OrderAmt 才会被使用
        OLTPClient.isOnlineDDL = true;
        // 2) 清空旧数据（如果需要重复跑的话）
        OLTPClient.sampler4OrderAmt = new ReservoirSampler<>(2_000_000);

        try (Connection conn = DriverManager.getConnection(DB_URL, USER, PASS)) {
            conn.setAutoCommit(true);

            // 3) 全表扫描 vodka_oorder.ol_amount_sum, 填蓄水池
            System.out.println("[SAMPLER] Scanning ol_amount_sum into ReservoirSampler …");
            try (Statement st = conn.createStatement();
                    ResultSet rs = st.executeQuery("SELECT ol_amount_sum FROM vodka_oorder")) {
                long cnt = 0;
                while (rs.next()) {
                    OLTPClient.sampler4OrderAmt.add(rs.getDouble(1));
                    cnt++;
                }
                System.out.println("[SAMPLER] Added " + cnt + " values to sampler4OrderAmt");
            }
            OLTPClient.sampler4OrderAmt.sortForInitialize();
            // 4) 为过滤率查询准备好语句
            PreparedStatement ratioStmt = conn.prepareStatement(
                    "SELECT COUNT(*) FILTER (WHERE ol_amount_sum > ?)::numeric / COUNT(*) AS ratio "
                            + "FROM vodka_oorder");

            // 5) 随机挑 10 个 q ∈ [0.7,1.0)，用 sampler 取阈值，再去库里验证
            Random rand = new Random(42);
            System.out.printf("%-8s %-12s %-8s%n", "Quantile", "Threshold", "Ratio");
            for (int i = 0; i < 10; i++) {
                double q = 0.7 + rand.nextDouble() * 0.3; // q ∈ [0.7,1.0)
                Double threshold = OLTPClient.sampler4OrderAmt.getPercentile(q);
                if (threshold == null) {
                    System.out.printf("%-8.3f %-12s %-8s%n", q, "null", "skip");
                    continue;
                }
                ratioStmt.setDouble(1, threshold);
                try (ResultSet rs = ratioStmt.executeQuery()) {
                    rs.next();
                    double ratio = rs.getDouble("ratio");
                    System.out.printf("%-8.3f %-12.2f %-8.3f%n", q, threshold, ratio);
                }
            }
        }
    }
}