package benchmark.olap.query;

import benchmark.olap.OLAPTerminal;
import benchmark.oltp.OLTPClient;
import config.CommonConfig;
import org.apache.log4j.Logger;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import static benchmark.oltp.OLTPClient.gloabalSysCurrentTime;
import static config.CommonConfig.DB_OCEANBASE;

public class Q8 extends baseQuery {
    private static Logger log = Logger.getLogger(Q8.class);
    public double k;
    public double b;
    private int dbType;

    public Q8(int dbType) throws ParseException {
        super();
        this.k = OLTPClient.k1;
        this.b = OLTPClient.b1;
        this.filterRate = benchmark.olap.OLAPClient.filterRate[7]; // o_entry_d=0.3115
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
        return String.valueOf(OLTPClient.sampler4Order.getPercentile(filterRate).getO_entry_d());
    }

    private int stage1 = 1 - 1;
    private int stage2 = 3 - 1;
    private int stage3 = 5 - 1;
    @Override
    public String getQuery() throws ParseException {
        // 1. TiDB Hint for inner query (with dynamic customer table)
        String innerHint = (dbType == CommonConfig.DB_TIDB)
                ? "/*+ read_from_storage(tiflash[vodka_item, vodka_supplier, vodka_order_line, vodka_oorder, %s, vodka_nation, vodka_nation, vodka_region]) */ "
                : "";

        // 2. Dynamic customer table based on stage3
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

        // 5. Single-format template
        String template = """
                select
                    o_year,
                    sum(case
                            when nation = 'BRAZIL' then volume
                            else 0
                        end) / (sum(volume) + 0.001) as mkt_share
                from (
                    select %s
                        extract(year from o_entry_d)       as o_year,
                        ol_amount * (1 - ol_discount)      as volume,
                        n2.n_name                         as nation
                    from
                        vodka_item,
                        vodka_supplier,
                        vodka_order_line,
                        vodka_oorder,
                        %s,
                        vodka_nation n1,
                        vodka_nation n2,
                        vodka_region
                    where
                        i_id            = ol_i_id
                        and s_suppkey   = ol_suppkey
                        and ol_w_id     = o_w_id
                        and ol_d_id     = o_d_id
                        and ol_o_id     = o_id
                        and c_w_id      = o_w_id
                        and c_d_id      = o_d_id
                        and c_id        = o_c_id
                        and c_nationkey = n1.n_nationkey
                        and n1.n_regionkey = r_regionkey
                        and r_name      = 'AMERICA'
                        and s_nationkey = n2.n_nationkey
                        and o_entry_d < %s%s
                        and i_type      = 'ECONOMY ANODIZED STEEL'
                        %s
                ) as all_nations
                group by
                    o_year
                order by
                    o_year;
                """;

        // 6. Fill in hints and variables
        String formattedHint = String.format(innerHint, customerTable);
        return String.format(template,
                formattedHint,
                customerTable,
                timeLiteral, extraMain,
                extraSub);
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
                    "(select count(*) from vodka_oorder where o_entry_d < TIMESTAMP '" + this.dynamicParam + "' ) " +
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
