package benchmark.olap.query;

import benchmark.olap.OLAPTerminal;
import benchmark.oltp.OLTPClient;
import config.CommonConfig;
import org.apache.log4j.Logger;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import static config.CommonConfig.DB_OCEANBASE;

public class Q6 extends baseQuery {
    private static Logger log = Logger.getLogger(Q6.class);
    public double k;
    public double b;
    private int dbType;

    public Q6(int dbType) throws ParseException {
        super();
        this.k = OLTPClient.k2;
        this.b = OLTPClient.b2;
        this.filterRate = benchmark.olap.OLAPClient.filterRate[5]; // ol_delivery_d0.1075
        this.dbType = dbType;
        this.dynamicParam = getDeltaTimes();
        this.q = getQuery();
    }

    public String updateQuery() throws ParseException {
        // this.orderlineTSize= OLAPTerminal.orderLineTableSize;
        // this.orderTSize=OLAPTerminal.oorderTableSize;
        // this.olNotnullSize=OLAPTerminal.orderlineTableNotNullSize;
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
    public String getQuery() throws ParseException {
        // 1. TiDB Hint
        String hint = (dbType == CommonConfig.DB_TIDB)
                ? "/*+ read_from_storage(tiflash[vodka_order_line]) */ "
                : "";

        // 2. 阶段性谓词（order_line 对应 stage1）
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
                    sum(ol_amount * ol_discount) as revenue
                from
                    vodka_order_line
                where
                    ol_delivery_d < %s
                    and ol_discount between .06 - 0.01 and .06 + 0.01
                    and ol_quantity < 24%s;
                """;

        return String.format(template,
                hint,
                timeLiteral,
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
                    "(select count(*) from vodka_order_line where ol_delivery_d < TIMESTAMP '" + this.dynamicParam
                    + "' )" +
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
