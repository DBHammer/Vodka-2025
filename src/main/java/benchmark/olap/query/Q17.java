package benchmark.olap.query;

import config.CommonConfig;

import java.text.ParseException;

import benchmark.oltp.OLTPClient;

import static config.CommonConfig.DB_OCEANBASE;

public class Q17 extends baseQuery {
    public double k;
    public double b;
    private int dbType;

    public Q17(int dbType) throws ParseException {
        super();
        this.filterRate = benchmark.olap.OLAPClient.filterRate[16];
        this.dbType = dbType;
        this.q = getQuery();
    }

    public String updateQuery() {
        // this.dynamicParam = getDeltaTimes();
        // this.q = getQuery();
        return this.q;
    }

    private int stage1 = 1 - 1;

    @Override
    public String getQuery() {
        // 1. Stage-1 predicate on order_line
        String extraSub = (OLTPClient.isOnlineDDL && OLTPClient.CURRENT_STAGE.get() >= stage1)
                ? " AND ol_tax <= 0.10"
                : "";

        // 2. Single-template + String.format (Postgres style, no hints)
        String template = """
                select
                    sum(ol_amount) / 7.0 as avg_yearly
                from
                    vodka_order_line,
                    vodka_item
                where
                    i_id = ol_i_id
                    and i_brand = 'Brand#23'
                    and i_container = 'MED BOX'
                    and ol_quantity < (
                        select
                            0.2 * avg(ol_quantity)
                        from
                            vodka_order_line
                        where
                            ol_i_id = i_id
                    )%s;
                """;

        return String.format(template, extraSub);
    }

    // @Override
    // public String getCountQuery() {
    // String q_str = "";
    // if (benchmark.olap.OLAPTerminal.countCheck) {
    // q_str = "select count(*) " +
    // "from " +
    // " vodka_order_line, " +
    // " vodka_item " +
    // "where " +
    // " i_id = ol_i_id " +
    // " and i_brand = 'Brand#23' " +
    // " and i_container = 'MED BOX' " +
    // " and ol_quantity < (" +
    // " select " +
    // " 0.2 * avg(ol_quantity) " +
    // " from " +
    // " vodka_order_line " +
    // " where " +
    // " ol_i_id = i_id" +
    // ");";
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
        return "";
    }

    @Override
    public String getDetailedExecutionPlan() {
        return "explain (analyze,costs false, timing false, summary false, format json) " + this.q;
    }
}
