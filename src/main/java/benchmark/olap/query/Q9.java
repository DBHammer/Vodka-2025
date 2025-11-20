package benchmark.olap.query;

import config.CommonConfig;

import java.text.ParseException;

import benchmark.oltp.OLTPClient;

import static config.CommonConfig.DB_OCEANBASE;

public class Q9 extends baseQuery {
    public double k;
    public double b;
    private int dbType;

    public Q9(int dbType) throws ParseException {
        super();
        this.filterRate = benchmark.olap.OLAPClient.filterRate[8];
        this.dbType = dbType;
        this.q = getQuery();
    }

    public String updateQuery() {
        return this.q;
    }

    private int stage1 = 1 - 1;
    private int stage2 = 3 - 1;

    @Override
    public String getQuery() {
        // 1. TiDB Hint
        String hint = (dbType == CommonConfig.DB_TIDB)
                ? "/*+ read_from_storage(tiflash[vodka_item, vodka_supplier, vodka_oorder, vodka_order_line, vodka_stock, vodka_nation]) */ "
                : "";

        // 2. Stage predicates
        String extraSub = (OLTPClient.isOnlineDDL && OLTPClient.CURRENT_STAGE.get() >= stage1)
                ? " AND ol_tax <= 0.10"
                : "";
        String extraMain = (OLTPClient.isOnlineDDL && OLTPClient.CURRENT_STAGE.get() >= stage2)
                ? " AND ol_amount_sum >= 0"
                : "";

        // 3. Single-format template
        String template = """
                select
                    nation,
                    o_year,
                    sum(amount) as sum_profit
                from (
                    select %s
                        n_name                                 as nation,
                        extract(year from o_entry_d)           as o_year,
                        ol_amount * (1 - ol_discount)
                            - s_supplycost * ol_quantity      as amount
                    from
                        vodka_item,
                        vodka_supplier,
                        vodka_order_line,
                        vodka_stock,
                        vodka_oorder,
                        vodka_nation
                    where
                        ol_suppkey = vodka_supplier.s_suppkey
                        AND ol_i_id = vodka_stock.s_i_id
                        AND ol_supply_w_id = vodka_stock.s_w_id 
                        and i_id = ol_i_id
                        and o_w_id = ol_w_id
                        and o_d_id = ol_d_id
                        and o_id   = ol_o_id
                        and s_nationkey = n_nationkey
                        and i_name like '%%green%%'
                        %s%s
                ) as profit
                group by
                    nation,
                    o_year
                order by
                    nation,
                    o_year desc;
                """;

        return String.format(template,
                hint,
                extraMain,
                extraSub);
    }
    // / " where " +
    // " s_suppkey = ol_suppkey " +
    // " and s_tocksuppkey = ol_suppkey " +
    // @Override
    // public String getCountQuery() {
    // String q_str = "";
    // if (benchmark.olap.OLAPTerminal.countCheck) {
    // q_str = "select count(*) " +
    // "from " +
    // " ( " +
    // " select " +
    // " n_name as nation, " +
    // " extract(year from o_entry_d) as o_year, " +
    // " ol_amount * (1 - ol_discount) - s_supplycost * ol_quantity as amount " +
    // " from " +
    // " vodka_item, " +
    // " vodka_supplier, " +
    // " vodka_order_line, " +
    // " vodka_stock, " +
    // " vodka_oorder, " +
    // " vodka_nation " +
    // " where " +
    // " s_suppkey = ol_suppkey " +
    // " and s_tocksuppkey = ol_suppkey " +
    // " and s_i_id = ol_i_id " +
    // " and i_id = ol_i_id " +
    // " and o_w_id = ol_w_id and o_d_id = ol_d_id and o_id = ol_o_id " +
    // " and s_nationkey = n_nationkey " +
    // " and i_name like '%green%' " +
    // " ) as profit;";
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
