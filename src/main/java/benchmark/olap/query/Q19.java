package benchmark.olap.query;

import java.text.ParseException;
import java.util.Map;

import benchmark.oltp.OLTPClient;

import static config.CommonConfig.DB_OCEANBASE;

public class Q19 extends baseQuery {
    private final int dbType;
    private final int low1, high1, low2, high2, low3, high3;

    public Q19(int dbType) throws ParseException {
        super();
        this.dbType = dbType;

        // 动态阈值映射表：filterRate → 每个品牌的 [low, high] 区间
        Map<Double, int[][]> thresholds = Map.of(
                0.25, new int[][] { { 1, 5 }, { 5, 10 }, { 10, 15 } },
                0.50, new int[][] { { 1, 8 }, { 8, 16 }, { 16, 24 } },
                1.00, new int[][] { { 1, 11 }, { 10, 20 }, { 20, 30 } },
                2.00, new int[][] { { 5, 15 }, { 15, 25 }, { 25, 35 } },
                4.00, new int[][] { { 10, 20 }, { 20, 30 }, { 30, 40 } });

        double filterRate = OLTPClient.scaleFilterRatio;
        int[][] th = thresholds.getOrDefault(filterRate, thresholds.get(1.00));

        // 每个品牌的数量区间
        this.low1 = th[0][0];
        this.high1 = th[0][1];
        this.low2 = th[1][0];
        this.high2 = th[1][1];
        this.low3 = th[2][0];
        this.high3 = th[2][1];

        this.q = getQuery();
    }

    @Override
    public String getQuery() {
        return String.format("""
                select
                    sum(ol_amount * (1 - ol_discount)) as revenue
                from
                    vodka_order_line,
                    vodka_item
                where
                    i_id = ol_i_id
                  and (
                        (
                            i_brand = 'Brand#12'
                            and i_container in ('SM CASE','SM BOX','SM PACK','SM PKG')
                            and ol_quantity between %d and %d
                            and i_size between 1 and 5
                            and ol_shipmode in ('AIR','AIR REG')
                            and ol_shipinstruct = 'DELIVER IN PERSON'
                        )
                        or
                        (
                            i_brand = 'Brand#23'
                            and i_container in ('MED BAG','MED BOX','MED PKG','MED PACK')
                            and ol_quantity between %d and %d
                            and i_size between 1 and 10
                            and ol_shipmode in ('AIR','AIR REG')
                            and ol_shipinstruct = 'DELIVER IN PERSON'
                        )
                        or
                        (
                            i_brand = 'Brand#34'
                            and i_container in ('LG CASE','LG BOX','LG PACK','LG PKG')
                            and ol_quantity between %d and %d
                            and i_size between 1 and 15
                            and ol_shipmode in ('AIR','AIR REG')
                            and ol_shipinstruct = 'DELIVER IN PERSON'
                        )
                    );
                """, low1, high1, low2, high2, low3, high3);
    }

    @Override
    public String getExplainQuery() {
        return (dbType == DB_OCEANBASE ? "EXPLAIN EXTENDED " : "EXPLAIN ANALYZE ") + this.q;
    }

    @Override
    public String getFilterCheckQuery() {
        return "";
    }

    @Override
    public String getDetailedExecutionPlan() {
        return "explain (analyze,costs false,timing false,summary false,format json) " + this.q;
    }

    public String updateQuery() {
        return this.q;
    }
}