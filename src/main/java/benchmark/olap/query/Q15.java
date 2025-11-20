package benchmark.olap.query;

import benchmark.olap.OLAPTerminal;
import benchmark.oltp.OLTPClient;
import config.CommonConfig;
import org.apache.log4j.Logger;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import static config.CommonConfig.DB_OCEANBASE;

public class Q15 extends baseQuery {
    private static Logger log = Logger.getLogger(Q15.class);
    public double k;
    public double b;
    private final int dbType;

    public Q15(int dbType) throws ParseException {
        super();
        this.filterRate = benchmark.olap.OLAPClient.filterRate[14];
        this.dbType = dbType;
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

    @Override
    public String getQuery() {
        // 1. Stage-1 predicate on order_line
        String extraSub = (OLTPClient.isOnlineDDL && OLTPClient.CURRENT_STAGE.get() >= stage1)
                ? " AND ol_tax <= 0.10"
                : "";

        // 2. Postgres timestamp literal
        String timeLiteral = "TIMESTAMP '" + this.dynamicParam + "'";

        // 3. Single-template + String.format
        String template = """
                with revenue (supplier_no, total_revenue) as (
                    select
                        ol_suppkey,
                        sum(ol_amount * (1 - ol_discount)) as total_revenue
                    from
                        vodka_order_line
                    where
                        ol_delivery_d < %s%s
                    group by
                        ol_suppkey
                )
                select
                    s_suppkey,
                    s_name,
                    s_address,
                    s_phone,
                    total_revenue
                from
                    vodka_supplier,
                    revenue
                where
                    s_suppkey = supplier_no
                    and total_revenue = (
                        select max(total_revenue) from revenue
                    )
                order by
                    s_suppkey;
                """;

        return String.format(template, timeLiteral, extraSub);
    }

    @Override
    public String getExplainQuery() {
        if (dbType == DB_OCEANBASE) {
            return "EXPLAIN EXTENDED " + this.q;
        }
        return "EXPLAIN ANALYZE " + this.q;
    }

    @Override
    public String getFilterCheckQuery() {
        if (benchmark.olap.OLAPTerminal.filterRateCheck) {
            return "select ( " +
                    "(select count(*) from vodka_order_line where ol_delivery_d < TIMESTAMP '" + this.dynamicParam
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
