package benchmark.olap.query;

import config.CommonConfig;
import java.text.ParseException;
import java.util.List;
import java.util.stream.Collectors;

import benchmark.oltp.OLTPClient;

import static config.CommonConfig.DB_OCEANBASE;

public class Q22 extends baseQuery {
    private final String inClause; // 动态生成的 countrycode 列表
    private final int dbType;

    public Q22(int dbType) throws ParseException {
        super();
        this.dbType = dbType;
        double rate = OLTPClient.scaleFilterRatio;
        List<Integer> codes;
        if (rate <= 0.25) {
            codes = List.of(13, 31);
        } else if (rate <= 0.5) {
            codes = List.of(13, 31, 23, 29);
        } else if (rate <= 1.0) {
            codes = List.of(13, 31, 23, 29, 30, 18, 17);
        } else if (rate <= 2.0) {
            codes = List.of(13, 31, 23, 29, 30, 18, 17,
                    2, 40, 52, 78, 76, 46, 62);
        } else {
            codes = List.of(
                    13, 31, 23, 29, 30, 18, 17,
                    2, 40, 52, 78, 76, 46, 62,
                    81, 83, 70, 19, 94, 98, 61,
                    9, 99, 97, 67, 43, 91, 21);
        }

        this.inClause = codes.stream()
                .map(c -> "'" + c + "'")
                .collect(Collectors.joining(","));

        this.filterRate = rate;
        this.q = getQuery();
    }

    @Override
    public String updateQuery() {
        return this.q;
    }

    private int stage3 = 5 - 1;

    @Override
    public String getQuery() {
        // 1. Dynamic customer table (stage3)
        String customerTable = (OLTPClient.isOnlineDDL && OLTPClient.CURRENT_STAGE.get() >= stage3)
                ? "vodka_customer_private"
                : "vodka_customer";

        // 2. Country codes list (already inClause)
        String codes = inClause;

        // 3. Single-template + String.format (Postgres style, no hints)
        String template = """
                select
                    cntrycode,
                    count(*)    as numcust,
                    sum(c_balance) as totacctbal
                from (
                    select
                        substring(c_phone from 1 for 2) as cntrycode,
                        c_balance
                    from
                        %s
                    where
                        substring(c_phone from 1 for 2) in (%s)
                        and c_balance > (
                            select avg(c_balance)
                            from %s
                            where c_balance > 0.00
                              and substring(c_phone from 1 for 2) in (%s)
                        )
                        and not exists (
                            select 1
                            from vodka_oorder
                            where c_w_id = o_w_id
                              and c_d_id = o_d_id
                              and c_id   = o_c_id
                        )
                ) as custsale
                group by
                    cntrycode
                order by
                    cntrycode;
                """;

        return String.format(template,
                customerTable, codes,
                customerTable, codes);
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