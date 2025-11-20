package benchmark.olap.query;

import benchmark.olap.OLAPTerminal;
import benchmark.oltp.OLTPClient;
import config.CommonConfig;
import org.apache.log4j.Logger;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import static config.CommonConfig.DB_OCEANBASE;

public class Q4 extends baseQuery {
    private static Logger log = Logger.getLogger(Q4.class);
    public double k;
    public double b;
    private int dbType;

    public Q4(int dbType) throws ParseException {
        super();
        this.k = OLTPClient.k1;
        this.b = OLTPClient.b1;
        this.filterRate = benchmark.olap.OLAPClient.filterRate[3]; // o_entry_d
        this.dbType = dbType;
        this.dynamicParam = getDeltaTimes();
        this.q = getQuery();
    }

    public String updateQuery() throws ParseException {
        // this.orderlineTSize=OLAPTerminal.orderLineTableSize;
        // this.orderTSize= OLAPTerminal.oorderTableSize;
        // this.olNotnullSize=OLAPTerminal.orderlineTableNotNullSize;
        this.k = OLTPClient.k1;
        this.b = OLTPClient.b1;
        this.dynamicParam = getDeltaTimes();
        this.q = getQuery();
        return this.q;
    }

    public String getDeltaTimes() throws ParseException {
        return String.valueOf(OLTPClient.sampler4Order.getPercentile(filterRate).getO_entry_d());
    }

    private int stage1 = 1 - 1;
    private int stage2 = 3 - 1;

    @Override
    public String getQuery() throws ParseException {
        // 1. Hint：TiDB 下加，否则留空
        String hintMain = (dbType == CommonConfig.DB_TIDB)
                ? "/*+ read_from_storage(tiflash[vodka_oorder]) */ "
                : "";
        String hintSub = (dbType == CommonConfig.DB_TIDB)
                ? "/*+ read_from_storage(tiflash[vodka_order_line]) */ "
                : "";

        // 2. 阶段性谓词（原始谓词未改动）
        String extraMain = (OLTPClient.isOnlineDDL && OLTPClient.CURRENT_STAGE.get() >= stage2)
                ? " AND ol_amount_sum >= 0"
                : "";
        String extraSub = (OLTPClient.isOnlineDDL && OLTPClient.CURRENT_STAGE.get() >= stage1)
                ? " AND ol_tax <= 0.10"
                : "";

        // 3. 时间字面量
        String timeLiteral = (dbType == CommonConfig.DB_TIDB)
                ? "'" + this.dynamicParam + "'"
                : "TIMESTAMP '" + this.dynamicParam + "'";

        // 4. 单一模板 + String.format
        String template = """
                select %s
                    o_carrier_id,
                    count(*) as order_count
                from
                    vodka_oorder
                where
                    o_entry_d < %s%s
                    and exists (
                        select %s
                            *
                        from
                            vodka_order_line
                        where
                            ol_w_id = o_w_id
                            and ol_d_id = o_d_id
                            and ol_o_id = o_id and ol_commitdate < ol_receipdate %s
                    )
                group by
                    o_carrier_id
                order by
                    o_carrier_id;
                """;

        return String.format(template,
                hintMain,
                timeLiteral, extraMain,
                hintSub, extraSub);
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
    // " and o_entry_d < date '" + this.dynamicParam + "' + " + " interval '3' month
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
