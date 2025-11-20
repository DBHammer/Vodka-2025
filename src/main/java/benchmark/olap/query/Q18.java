package benchmark.olap.query;

import config.CommonConfig;
import benchmark.olap.OLAPClient;
import benchmark.oltp.OLTPClient;

import java.text.ParseException;

import static config.CommonConfig.DB_OCEANBASE;

public class Q18 extends baseQuery {
    public double k;
    public double b;
    private int dbType;
    private int quantityThreshold;

    public Q18(int dbType) throws ParseException {
        super();
        this.dbType = dbType;
        double rate = OLTPClient.scaleFilterRatio;
        // System.out.println(rate);
        if (rate <= 0.25) {
            this.quantityThreshold = 392;
        } else if (rate <= 0.5) {
            this.quantityThreshold = 355;
        } else if (rate <= 1.0) {
            this.quantityThreshold = 300;
        } else if (rate <= 2.0) {
            this.quantityThreshold = 206;
        } else if (rate <= 4.0) {
            this.quantityThreshold = 114;
        } else {
            this.quantityThreshold = 300;
        }
        // System.out.println(this.quantityThreshold);
        this.filterRate = benchmark.olap.OLAPClient.filterRate[17];
        this.q = getQuery();
    }

    public String updateQuery() {
        return this.q;
    }

    private int stage1 = 1 - 1;
    private int stage2 = 3 - 1;
    @Override
    public String getQuery() {
        // 1. TiDB hints (empty for Postgres)
        String hintSub = (dbType == CommonConfig.DB_TIDB)
                ? "/*+ read_from_storage(tiflash[vodka_order_line]) */ "
                : "";
        String hintMain = (dbType == CommonConfig.DB_TIDB)
                ? "/*+ read_from_storage(tiflash[vodka_customer, vodka_oorder, vodka_order_line]) */ "
                : "";

        // 2. Stage predicates
        String extraSub = (OLTPClient.isOnlineDDL && OLTPClient.CURRENT_STAGE.get() >= stage1)
                ? " where ol_tax <= 0.10"
                : "";
        String extraMain = (OLTPClient.isOnlineDDL && OLTPClient.CURRENT_STAGE.get() >= stage2)
                ? " AND ol_amount_sum >= 0"
                : "";

        // 3. Quantity threshold
        int qt = this.quantityThreshold;

        // 4. Single-template + String.format
        String template = """
                select %s
                    c_last,
                    c_w_id, c_d_id, c_id,
                    o_w_id, o_d_id, o_id,
                    o_entry_d,
                    sum(ol_quantity) as o_totalprice
                from
                    vodka_customer,
                    vodka_oorder,
                    vodka_order_line
                where
                    (o_w_id, o_d_id, o_id) in (
                        select %s
                            ol_w_id, ol_d_id, ol_o_id
                        from
                            vodka_order_line
                            %s
                        group by
                            ol_w_id, ol_d_id, ol_o_id
                        having
                            sum(ol_quantity) > %d
                    )
                    and c_w_id    = o_w_id
                    and c_d_id    = o_d_id
                    and c_id      = o_c_id
                    and ol_w_id   = o_w_id
                    and ol_d_id   = o_d_id
                    and ol_o_id   = o_id %s
                group by
                    c_last,
                    c_w_id, c_d_id, c_id,
                    o_w_id, o_d_id, o_id,
                    o_entry_d
                order by
                    o_totalprice desc,
                    o_entry_d
                limit 5;
                """;

        return String.format(template,
                hintMain,
                hintSub, extraSub,
                qt,
                extraMain);
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
        return "";
    }

    @Override
    public String getDetailedExecutionPlan() {
        return "explain (analyze,costs false, timing false, summary false, format json) " + this.q;
    }
}