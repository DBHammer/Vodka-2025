package benchmark.olap.query;

import benchmark.olap.OLAPTerminal;
import benchmark.oltp.OLTPClient;
import config.CommonConfig;
import org.apache.log4j.Logger;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import static config.CommonConfig.DB_OCEANBASE;
import static config.CommonConfig.DB_POSTGRES;

public class Q12 extends baseQuery {
    private static Logger log = Logger.getLogger(Q12.class);
    public double k;
    public double b;
    private int dbType;

    public Q12(int dbType) throws ParseException {
        super();
        this.k = OLTPClient.k2;
        this.b = OLTPClient.b2;
        this.filterRate = benchmark.olap.OLAPClient.filterRate[11]; // ol_receipdate=0.1518
        this.dbType = dbType;
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
        double countNumber = OLAPTerminal.orderLineTableSize.get() * filterRate;

        SimpleDateFormat simFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date start_d = simFormat.parse("1998-08-02 00:00:00");// min date 1992-01-01,1994-01-01
        Date start_d_1 = simFormat.parse("1992-01-01 00:00:00");

        if (countNumber >= OLAPTerminal.orderlineTableRecipDateNotNullSize.get()) {
            int s = (int) (((countNumber - this.b) / this.k) / 1000);
            return simFormat.format(super.getDateAfter(start_d, s));
        } else {
            double historyNumber = OLAPTerminal.orderlineTableRecipDateNotNullSize.get() - countNumber;
            int s = (int) ((1 - historyNumber / OLAPTerminal.orderlineTableRecipDateNotNullSize.get())
                    * ((2405 + OLTPClient.deltaDays2) * 24 * 60 * 60));
            return simFormat.format(super.getDateAfter(start_d_1, s));
        }
    }

    private int stage1 = 1 - 1;
    private int stage2 = 3 - 1;

    @Override
    public String getQuery() throws ParseException {
        // 1. TiDB Hint
        String hint = (dbType == CommonConfig.DB_TIDB)
                ? "/*+ read_from_storage(tiflash[vodka_oorder, vodka_order_line]) */ "
                : "";

        // 2. 阶段性谓词
        String extraSub = (OLTPClient.isOnlineDDL && OLTPClient.CURRENT_STAGE.get() >= stage1)
                ? " AND ol_tax <= 0.10"
                : "";
        String extraMain = (OLTPClient.isOnlineDDL && OLTPClient.CURRENT_STAGE.get() >= stage2)
                ? " AND ol_amount_sum >= 0"
                : "";

        // 3. 时间字面量
        String timeLiteral = (dbType == CommonConfig.DB_TIDB)
                ? "'" + this.dynamicParam + "'"
                : "TIMESTAMP '" + this.dynamicParam + "'";

        // 4. 单次 String.format 模板
        String template = """
                select %s
                    ol_shipmode,
                    sum(case
                            when o_carrier_id = 1 or o_carrier_id = 2 then 1
                            else 0
                        end) as high_line_count,
                    sum(case
                            when o_carrier_id <> 1 and o_carrier_id <> 2 then 1
                            else 0
                        end) as low_line_count
                from
                    vodka_oorder,
                    vodka_order_line
                where
                    ol_w_id = o_w_id
                    and ol_d_id = o_d_id
                    and ol_o_id = o_id
                    and ol_shipmode in ('MAIL', 'SHIP')
                    and ol_commitdate < ol_receipdate
                    and ol_delivery_d < ol_commitdate
                    and ol_receipdate < %s%s%s
                group by
                    ol_shipmode
                order by
                    ol_shipmode;
                """;

        return String.format(template,
                hint,
                timeLiteral,
                extraMain,
                extraSub);
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
            // return "select ( " +
            // "(select count(*) from vodka_order_line where ol_receipdate < date '" +
            // this.dynamicParam + "' and ol_commitdate < ol_receipdate and ol_delivery_d <
            // ol_commitdate) " +
            // "/ " +
            // "(select count(*) from vodka_order_line) " +
            // ");";ol_receipdate < TIMESTAMP '1994-01-06 01:52:22.068'
            if (dbType == DB_POSTGRES)
                return "SELECT COUNT(*) FILTER (where ol_receipdate <  TIMESTAMP '" + this.dynamicParam + "'"
                        + ")::NUMERIC / COUNT(*) FROM vodka_order_line where ol_receipdate IS NOT NULL;";
            return "select ( " +
                    "(select count(*) from vodka_order_line where ol_receipdate < date '" + this.dynamicParam + "'" +
                    "/ " +
                    "(select count(*) from vodka_order_line where ol_receipdate IS NOT NULL" +
                    ");";
        }
        return "";
    }

    @Override
    public String getDetailedExecutionPlan() {
        return "explain (analyze,costs false, timing false, summary false, format json) " + this.q;
    }
}
