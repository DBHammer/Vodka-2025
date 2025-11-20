package benchmark.olap.query;

import benchmark.olap.OLAPTerminal;
import benchmark.oltp.OLTPClient;
import config.CommonConfig;
import org.apache.log4j.Logger;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Date;

import static benchmark.oltp.OLTPClient.gloabalSysCurrentTime;

import static config.CommonConfig.DB_OCEANBASE;

public class Q14 extends baseQuery {
    private static Logger log = Logger.getLogger(Q14.class);
    public double k;
    public double b;
    private int dbType;
    private String dynamicParam1;
    private String dynamicParam2;

    public Q14(int dbType) throws ParseException {
        super();
        this.k = OLTPClient.k2;
        this.b = OLTPClient.b2;
        this.filterRate = benchmark.olap.OLAPClient.filterRate[13]; // ol_delivery_d=0.0087
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
        // return String.valueOf("2025-08-11 13:25:20.472");
        int mode = 2;
        if (mode == 0) {
            long tsMillis = OLTPClient.sessionStartTimestamp + 5000; // 假设这是毫秒
            Instant instant = Instant.ofEpochMilli(tsMillis);
            DateTimeFormatter fmt = DateTimeFormatter
                    .ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS")
                    .withZone(ZoneId.systemDefault());
            String tsLiteral = fmt.format(instant);
            System.out.println(tsLiteral);
            return tsLiteral;
        } else if (mode == 1) {
            return String.valueOf(OLTPClient.sampler4OrderLine.getRandomItem().getOl_delivery_d());
        } else
            return String.valueOf(OLTPClient.sampler4OrderLine.getPercentile(filterRate).getOl_delivery_d());
    }

    private int stage1 = 1 - 1;

    @Override
    public String getQuery() throws ParseException {
        // 1. Stage-1 predicate (order_line only)
        String extraSub = (OLTPClient.isOnlineDDL && OLTPClient.CURRENT_STAGE.get() >= stage1)
                ? " AND ol_tax <= 0.10"
                : "";
        // 2. Postgres timestamp literal
        String timeLiteral = "TIMESTAMP '" + this.dynamicParam + "'";

        // 3. Single-template + String.format (Postgres style, no hints)
        String template = """
                select
                    100.00 * sum(
                        case when i_type like 'PROMO%%'
                             then ol_amount * (1 - ol_discount) else 0
                        end
                    ) / (sum(ol_amount * (1 - ol_discount)) + 0.001)
                    as promo_revenue
                from
                    vodka_order_line,
                    vodka_item
                where
                    ol_i_id = i_id
                    and ol_delivery_d <= %s%s;
                """;
        // System.out.println(String.format(template, timeLiteral, extraSub));
        // and ol_delivery_d >= TIMESTAMP '1993-09-01 00:00:00.00000'
        return String.format(template, timeLiteral, extraSub);
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
