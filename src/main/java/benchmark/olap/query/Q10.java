package benchmark.olap.query;

import benchmark.oltp.OLTPClient;
import config.CommonConfig;

import org.apache.commons.math3.util.Pair;
import org.apache.log4j.Logger;

import bean.Order;

import java.text.ParseException;
import java.util.List;

import static config.CommonConfig.DB_OCEANBASE;

public class Q10 extends baseQuery {
    private static Logger log = Logger.getLogger(Q10.class);
    public double k;
    public double b;
    private int dbType;
    /* ---------- 新字段 ---------- */
    private final double subRate; // √filterRate
    private double amtThreshold; // 阈值数值
    private String amtThresholdLit; // 阈值字面量（SQL 拼接用）
    private int stage1 = 1 - 1;
    private int stage2 = 3 - 1;
    private int stage3 = 5 - 1;
    public Q10(int dbType) throws ParseException {
        super();
        this.filterRate = benchmark.olap.OLAPClient.filterRate[9]; // 0.2657
        // this.subRate = Math.sqrt(this.filterRate);
        // System.out.println("current is: " + filterRate);
        this.subRate = 0.35;
        this.dbType = dbType;
        refreshDynamicParams();
        this.q = getQuery();
    }

    /** 根据当前阶段刷新 o_entry_d / amtThreshold 两个字面量 */
    private void refreshDynamicParams() throws ParseException {
        this.dynamicParam = getDeltaTimes();
        /* —— 2. 阶段-2 时的金额阈值 —— */
        if (OLTPClient.isOnlineDDL) {
            // >= 4
            if (OLTPClient.CURRENT_STAGE.get() >= stage2) {
                this.amtThresholdLit = "0";
                // var percentileAmt = OLTPClient.sampler4OrderAmt.getPercentile(1 - subRate);
                // if (percentileAmt == null) {
                // log.warn("Percentile for amtThreshold is null, skipping threshold filter");
                // this.amtThresholdLit = null;
                // } else {
                // double amtCut = percentileAmt.doubleValue();
                // log.info("the value is: " + amtCut);
                // this.amtThreshold = amtCut;
                // this.amtThresholdLit = String.format("%.2f", amtCut);
                // }
                // o_entry_d 同样做 null 检查
                // double rate2 = filterRate / subRate + 0.01;
                double rate2 = filterRate;
                var percentileEntry = OLTPClient.sampler4Order.getPercentile(rate2);
                if (percentileEntry == null) {
                    log.warn(
                            "Percentile for dynamicParam is null, keeping previous dynamicParam: " +
                                    this.dynamicParam);
                } else {
                    this.dynamicParam = String.valueOf(percentileEntry.getO_entry_d());
                }
            } else {
                this.amtThresholdLit = null;
            }
            // 蓄水池样本（包含 o_entry_d 和 ol_amount_sum）
            // List<Order> sample = OLTPClient.sampler4Order.getSortedSample();
            // sample.sort((a, b) -> {
            // int cmp = a.getO_entry_d().compareTo(b.getO_entry_d());
            // if (cmp != 0)
            // return cmp;
            // return -Double.compare(a.getOl_amount_sum(), b.getOl_amount_sum());
            // });
            // int idx = (int) Math.ceil(sample.size() * filterRate);
            // idx = Math.min(sample.size() - 1, Math.max(0, idx));
            // Order cutoff = sample.get(idx);
            // this.dynamicParam = String.valueOf(cutoff.getO_entry_d());
            // this.amtThresholdLit = String.format("%.2f", cutoff.getOl_amount_sum());
        }
    }

    public String updateQuery() throws ParseException {
        refreshDynamicParams(); // ★ 一键刷新
        this.q = getQuery();
        return this.q;
    }

    public String getDeltaTimes() throws ParseException {
        return String.valueOf(OLTPClient.sampler4Order.getPercentile(filterRate).getO_entry_d());
    }

    @Override
    public String getQuery() throws ParseException {
        // return String.format("""
        // SELECT
        // COUNT(*) FILTER (WHERE ol_amount_sum >= %s)::NUMERIC / COUNT(*) AS rate_amt,
        // COUNT(*) FILTER (WHERE o_entry_d < TIMESTAMP '%s')::NUMERIC / COUNT(*) AS
        // rate_date,
        // COUNT(*) FILTER (WHERE ol_amount_sum >= %s
        // AND o_entry_d < TIMESTAMP '%s')::NUMERIC / COUNT(*) AS rate_both
        // FROM vodka_oorder;
        // """,
        // amtThresholdLit, // 第一个阈值
        // dynamicParam, // 时间字面量
        // amtThresholdLit,
        // dynamicParam);
        // ---------- 按阶段决定额外谓词 ----------
        String extra = "";
        String customerTable = "vodka_customer join ";
        if (OLTPClient.isOnlineDDL) {
            // if (OLTPClient.CURRENT_STAGE.get() >= 1,4,5) {
            if (OLTPClient.CURRENT_STAGE.get() >= stage1) {
                extra = " AND ol_tax <= 0.10\n";
                if (OLTPClient.CURRENT_STAGE.get() >= stage2) {
                    // extra += " AND ol_amount_sum > 0" + amtThresholdLit + "\n";
                    extra += " AND ol_amount_sum >= 0" + "\n";
                    if (OLTPClient.CURRENT_STAGE.get() >= stage3) {
                        customerTable = "vodka_customer_public join ";
                    }
                }
            }
        }

        // ---------- 时间字面量 ----------
        String timeLiteral = (dbType == CommonConfig.DB_TIDB)
                ? "'" + this.dynamicParam + "'"
                : "TIMESTAMP '" + this.dynamicParam + "'";

        // ---------- SQL 拼接 ----------
        String query = String.format("""
                select c_w_id,
                c_d_id,
                c_id,
                sum(ol_amount * (1 - ol_discount)) as revenue,
                n_name
                FROM %s
                vodka_oorder on (c_w_id = o_w_id and c_d_id = o_d_id and c_id = o_c_id)
                join vodka_order_line on (ol_w_id = o_w_id and ol_d_id = o_d_id and ol_o_id =
                o_id)
                join vodka_nation on (c_nationkey = n_nationkey)
                where o_entry_d < %s
                AND ol_returnflag = 'R'
                %s
                GROUP BY
                c_w_id, c_d_id, c_id, n_name
                ORDER BY revenue DESC
                LIMIT 20;
                """, customerTable, timeLiteral, extra);
        return query;
    }

    // @Override
    // public String getCountQuery() {
    // String q_str = "";
    // if (benchmark.olap.OLAPTerminal.countCheck) {
    // q_str = "select count(*) " +
    // "from " +
    // " vodka_oorder " +
    // "where " +
    // " o_entry_d >= date '" + this.dynamicParam + "' " +
    // " and o_entry_d < date '" + this.dynamicParam + "' + interval '3' month ;";
    // }
    // return q_str;
    // }

    @Override
    public String getExplainQuery() {
        switch (dbType) {
            case DB_OCEANBASE -> {
                return "EXPLAIN EXTENDED " + this.q;
            }
            default -> {
                return "EXPLAIN ANALYZE " + this.q;
            }
        }
    }

    @Override
    public String getFilterCheckQuery() {
        if (benchmark.olap.OLAPTerminal.filterRateCheck) {
            return "select ( " +
                    "(select count(*) from vodka_oorder where o_entry_d < TIMESTAMP '" + this.dynamicParam + "') " +
                    "/ " +
                    "(select count(*) from vodka_oorder) " +
                    ");";
        }
        return "";
    }

    @Override
    public String getDetailedExecutionPlan() {
        return "explain (analyze,costs false, timing false, summary false, format json) " + this.q;
    }
}
