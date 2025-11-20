package benchmark.olap.query;

import config.CommonConfig;

import java.text.ParseException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import benchmark.oltp.OLTPClient;

import static config.CommonConfig.DB_OCEANBASE;

public class Q16 extends baseQuery {
    private final int dbType;
    private final String sizeListStr;

    public Q16(int dbType) throws ParseException {
        super();
        this.dbType = dbType;
        double rate = OLTPClient.scaleFilterRatio;
        List<Integer> codes;
        if (rate <= 0.25) {
            codes = List.of(49, 14);
        } else if (rate <= 0.5) {
            codes = List.of(49, 14, 23, 45);
        } else if (rate <= 1.0) {
            codes = List.of(49, 14, 23, 45, 19, 3, 36);
        } else if (rate <= 2.0) {
            codes = List.of(49, 14, 23, 45, 19, 3, 36, 9, 1, 2, 4, 5, 6, 7, 8);
        } else {
            codes = List.of(
                    49, 14, 23, 45, 19, 3, 36, 9,
                    1, 2, 4, 5, 6, 7, 8,
                    10, 11, 12, 13, 15, 16, 17,
                    31, 20, 50, 22, 24, 33, 28);
        }
        this.sizeListStr = codes.stream()
                .map(String::valueOf)
                .collect(Collectors.joining(","));

        this.filterRate = rate;
        this.q = getQuery();
    }

    @Override
    public String updateQuery() {
        return this.q;
    }

    @Override
    public String getQuery() {
        String inClause = "(" + sizeListStr + ")";
        String query;
        switch (this.dbType) {
            case CommonConfig.DB_TIDB:
                query = "select /*+ read_from_storage(tiflash[vodka_stock, vodka_item]) */\n" +
                        "    i_brand,\n" +
                        "    i_type,\n" +
                        "    i_size,\n" +
                        "    count(distinct s_tocksuppkey) as supplier_cnt\n" +
                        "from\n" +
                        "    vodka_stock,\n" +
                        "    vodka_item\n" +
                        "where\n" +
                        "      i_id = s_i_id\n" +
                        "  and i_brand <> 'Brand#45'\n" +
                        "  and i_type not like 'MEDIUM POLISHED%'\n" +
                        "  and i_size in " + inClause + "\n" +
                        "  and s_tocksuppkey not in (\n" +
                        "    select /*+ read_from_storage(tiflash[vodka_supplier]) */\n" +
                        "        s_suppkey\n" +
                        "    from\n" +
                        "        vodka_supplier\n" +
                        "    where\n" +
                        "            s_comment like '%Customer%Complaints%'\n" +
                        "        and s_suppkey < 500\n" +
                        ")\n" +
                        "group by\n" +
                        "    i_brand,\n" +
                        "    i_type,\n" +
                        "    i_size\n" +
                        "order by\n" +
                        "    supplier_cnt desc,\n" +
                        "    i_brand,\n" +
                        "    i_type,\n" +
                        "    i_size;";
                break;
            default:
                query = "select\n" +
                        "    i_brand,\n" +
                        "    i_type,\n" +
                        "    i_size,\n" +
                        "    count(distinct s_tocksuppkey) as supplier_cnt\n" +
                        "from\n" +
                        "    vodka_stock,\n" +
                        "    vodka_item\n" +
                        "where\n" +
                        "      i_id = s_i_id\n" +
                        "  and i_brand <> 'Brand#45'\n" +
                        "  and i_type not like 'MEDIUM POLISHED%'\n" +
                        "  and i_size in " + inClause + "\n" +
                        "  and s_tocksuppkey not in (\n" +
                        "    select\n" +
                        "        s_suppkey\n" +
                        "    from\n" +
                        "        vodka_supplier\n" +
                        "    where\n" +
                        "            s_comment like '%Customer%Complaints%'\n" +
                        "        and s_suppkey < 500\n" +
                        ")\n" +
                        "group by\n" +
                        "    i_brand,\n" +
                        "    i_type,\n" +
                        "    i_size\n" +
                        "order by\n" +
                        "    supplier_cnt desc,\n" +
                        "    i_brand,\n" +
                        "    i_type,\n" +
                        "    i_size;";
                break;
        }
        return query;
    }

    @Override
    public String getExplainQuery() {
        return dbType == DB_OCEANBASE
                ? "EXPLAIN EXTENDED " + this.q
                : "EXPLAIN ANALYZE " + this.q;
    }

    @Override
    public String getFilterCheckQuery() {
        return "";
    }

    @Override
    public String getDetailedExecutionPlan() {
        return "explain (analyze,costs false,timing true,summary false,format json) " + this.q;
    }
}