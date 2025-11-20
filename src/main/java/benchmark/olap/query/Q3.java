package benchmark.olap.query;

import benchmark.olap.OLAPTerminal;
import benchmark.oltp.OLTPClient;
import config.CommonConfig;
import org.apache.log4j.Logger;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import static config.CommonConfig.DB_OCEANBASE;

public class Q3 extends baseQuery {
    private static final Logger log = Logger.getLogger(Q3.class);
    public double k;
    public double b;
    private final int dbType;

    public Q3(int dbType) throws ParseException {
        super();
        this.filterRate = benchmark.olap.OLAPClient.filterRate[2]; // 0.0480
        this.dbType = dbType;
        this.k = OLTPClient.k2;
        this.b = OLTPClient.b2;
        this.dynamicParam = getDeltaTimes();
        this.q = getQuery();
    }

    public String updateQuery() throws ParseException {
        this.k = OLTPClient.k2;
        this.b = OLTPClient.b2;
        this.dynamicParam = getDeltaTimes();
        this.q = getQuery();
        return this.q;
    }

    public String getDeltaTimes() throws ParseException {
        return String.valueOf(OLTPClient.sampler4OrderLine.getPercentile(filterRate).getOl_delivery_d());
    }

    private int stage1 = 1 - 1;
    private int stage2 = 3 - 1;
    private int stage3 = 5 - 1;
    @Override
    public String getQuery() throws ParseException {
        String extra = "";
        String customerTable = "vodka_customer";

        if (OLTPClient.isOnlineDDL) {
            if (OLTPClient.CURRENT_STAGE.get() >= stage1) {
                extra += " AND ol_tax <= 0.10";
            }
            if (OLTPClient.CURRENT_STAGE.get() >= stage2) {
                extra += " AND ol_amount >= 0"; // ol_amount_sum 不存在，用 ol_amount
            }
            if (OLTPClient.CURRENT_STAGE.get() >= stage3) {
                customerTable = "vodka_customer_public";
            }
        }

        String timeLiteral = (dbType == CommonConfig.DB_TIDB)
                ? "'" + this.dynamicParam + "'"
                : "TIMESTAMP '" + this.dynamicParam + "'";

        String hint = "/*+ read_from_storage(tiflash[" + customerTable + ", vodka_oorder, vodka_order_line]) */";

        String query = String.format("""
                select %s
                    ol_w_id, ol_d_id, ol_o_id,
                    sum(ol_amount * (1 - ol_discount)) as revenue,
                    o_entry_d,
                    o_shippriority
                from
                    %s,
                    vodka_order_line,
                    vodka_oorder
                where
                    c_mktsegment = 'BUILDING'
                    and c_w_id = o_w_id and c_d_id = o_d_id and c_id = o_c_id
                    and ol_w_id = o_w_id and ol_d_id = o_d_id and ol_o_id = o_id
                    and ol_delivery_d < %s
                    %s
                group by
                    ol_w_id, ol_d_id, ol_o_id,
                    o_entry_d,
                    o_shippriority
                order by
                    revenue desc,
                    o_entry_d
                limit 10;
                """, (dbType == CommonConfig.DB_TIDB ? hint : ""), customerTable, timeLiteral, extra);

        return query;
    }

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
                    "(select count(*) from vodka_order_line where ol_delivery_d > TIMESTAMP '" + this.dynamicParam
                    + "') " +
                    "/ " +
                    "(select count(*) from vodka_order_line) " +
                    ");";
        }
        return "";
    }

    @Override
    public String getDetailedExecutionPlan() {
        return "explain (analyze,costs false, timing false, summary false, format json) " + this.q;
    }
}
