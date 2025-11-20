package benchmark.olap.query;

import benchmark.olap.OLAPTerminal;
import benchmark.oltp.OLTPClient;
import config.CommonConfig;
import org.apache.log4j.Logger;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import static config.CommonConfig.DB_OCEANBASE;

public class Q7 extends baseQuery {
    private static Logger log = Logger.getLogger(Q7.class);
    public double k;
    public double b;
    private int dbType;

    public Q7(int dbType) throws ParseException {
        super();
        this.k = OLTPClient.k2;
        this.b = OLTPClient.b2;
        this.filterRate = benchmark.olap.OLAPClient.filterRate[6];// ol_delivery_d=0.2175
        this.dbType = dbType;
        this.dynamicParam = getDeltaTimes();
        this.q = getQuery();
    }

    public String updateQuery() throws ParseException {
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
        // 1. TiDB Hint for inner query
        String innerHint = (dbType == CommonConfig.DB_TIDB)
                ? "/*+ read_from_storage(tiflash[vodka_supplier, vodka_order_line, vodka_oorder, %s, vodka_nation, vodka_nation]) */ "
                : "";

        // 2. Choose customer table based on stage3
        String customerTable = (OLTPClient.isOnlineDDL && OLTPClient.CURRENT_STAGE.get() >= stage3)
                ? "vodka_customer_public"
                : "vodka_customer";

        // 3. Stage predicates
        String extraSub = (OLTPClient.isOnlineDDL && OLTPClient.CURRENT_STAGE.get() >= stage1)
                ? " AND ol_tax <= 0.10"
                : "";
        String extraMain = (OLTPClient.isOnlineDDL && OLTPClient.CURRENT_STAGE.get() >= stage2)
                ? " AND ol_amount_sum >= 0"
                : "";

        // 4. Time literal
        String timeLiteral = (dbType == CommonConfig.DB_TIDB)
                ? "'" + this.dynamicParam + "'"
                : "TIMESTAMP '" + this.dynamicParam + "'";

        // 5. Single template
        String template = """
                select
                    supp_nation,
                    cust_nation,
                    l_year,
                    sum(volume) as revenue
                from (
                    select %s
                        n1.n_name   as supp_nation,
                        n2.n_name   as cust_nation,
                        extract(year from ol_delivery_d) as l_year,
                        ol_amount * (1 - ol_discount) as volume
                    from
                        vodka_supplier,
                        vodka_order_line,
                        vodka_oorder,
                        %s,
                        vodka_nation n1,
                        vodka_nation n2
                    where
                        s_suppkey       = ol_suppkey
                        and ol_w_id     = o_w_id
                        and ol_d_id     = o_d_id
                        and ol_o_id     = o_id
                        and c_w_id      = o_w_id
                        and c_d_id      = o_d_id
                        and c_id        = o_c_id
                        and s_nationkey = n1.n_nationkey
                        and c_nationkey = n2.n_nationkey
                        and (
                            (n1.n_name = 'FRANCE' and n2.n_name = 'GERMANY')
                            or (n1.n_name = 'GERMANY' and n2.n_name = 'FRANCE')
                        )
                        and ol_delivery_d < %s%s
                        %s
                ) as shipping
                group by
                    supp_nation,
                    cust_nation,
                    l_year
                order by
                    supp_nation,
                    cust_nation,
                    l_year;
                """;

        // 6. Apply innerHint to include customerTable
        String formattedHint = String.format(innerHint, customerTable);

        return String.format(template,
                formattedHint,
                customerTable,
                timeLiteral, extraMain,
                extraSub);
    }

    // @Override
    // public String getCountQuery() {
    // String q_str = "";
    // if (benchmark.olap.OLAPTerminal.countCheck) {
    // q_str = "select count(*) " +
    // "from " +
    // "vodka_order_line " +
    // "where ol_delivery_d >= date '" + this.dynamicParam + "' " +
    // " and ol_delivery_d < date '" + this.dynamicParam + "' + interval '2' year
    // ;";
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
                    "(select count(*) from vodka_order_line where ol_delivery_d < TIMESTAMP '" + this.dynamicParam
                    + "' ) " +
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
