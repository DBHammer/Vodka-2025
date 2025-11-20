package utils.math.random;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 保存各列 Zipf 档位（0-4）。
 * <p>
 * 0 代表均匀分布，1-4 代表 Zipf 越来越陡。
 * </p>
 */
public final class ZipfConfig {

    /** 列名 → 档位（0‥4）。ConcurrentHashMap 方便运行时或单测动态调整 */
    // public static final Map<String, Integer> SKEW = new ConcurrentHashMap<>();

    public static final Map<String, Integer> SKEW = Map.ofEntries(
            // 0-4
            /* ---------- order-line ---------- */
            // Map.entry("vodka_order_line.ol_returnflag", 2),
            // Map.entry("vodka_order_line.ol_shipmode", 1),
            // Map.entry("vodka_order_line.ol_quantity", 2),
            // Map.entry("vodka_order_line.ol_shipinstruct", 4),
            // Map.entry("vodka_order_line.ol_discount", 3),

            // /* ---------- customer / order ---------- */
            // Map.entry("vodka_customer.c_phone", 4),
            // Map.entry("vodka_customer.c_balance", 0),
            // Map.entry("vodka_customer.c_nationkey", 4),
            // Map.entry("vodka_customer.c_mktsegment", 1),
            // Map.entry("vodka_order.o_comment", 0),

            // /* ---------- supplier / stock ---------- */
            // Map.entry("vodka_supplier.s_nationkey", 4),
            // Map.entry("vodka_stock.s_supplycost", 1),

            // /* ---------- item ---------- */
            // Map.entry("vodka_item.i_size", 3),
            // Map.entry("vodka_item.i_brand", 2),
            // Map.entry("vodka_item.i_container", 4),
            // Map.entry("vodka_item.i_type", 4),

            // /* ---------- nation / region ---------- */
            // Map.entry("vodka_nation.n_name", 4)
            /* ---------- order-line ---------- */
            // 0-1
            // Map.entry("vodka_order_line.ol_returnflag", 1),
            // Map.entry("vodka_order_line.ol_shipmode", 1),
            // Map.entry("vodka_order_line.ol_quantity", 1),
            // Map.entry("vodka_order_line.ol_shipinstruct", 1),
            // Map.entry("vodka_order_line.ol_discount", 1),

            // /* ---------- customer / order ---------- */
            // Map.entry("vodka_customer.c_phone", 1),
            // Map.entry("vodka_customer.c_balance", 0),
            // Map.entry("vodka_customer.c_nationkey", 1),
            // Map.entry("vodka_customer.c_mktsegment", 1),
            // Map.entry("vodka_order.o_comment", 0),

            // /* ---------- supplier / stock ---------- */
            // Map.entry("vodka_supplier.s_nationkey", 1),
            // Map.entry("vodka_stock.s_supplycost", 1),

            // /* ---------- item ---------- */
            // Map.entry("vodka_item.i_size", 1),
            // Map.entry("vodka_item.i_brand", 1),
            // Map.entry("vodka_item.i_container", 0),
            // Map.entry("vodka_item.i_type", 1),

            // /* ---------- nation / region ---------- */
            // Map.entry("vodka_nation.n_name", 4) // region.r_name 已按需求移除

            // 0-2
            // /* ---------- order-line ---------- */
            // Map.entry("vodka_order_line.ol_returnflag", 2),
            // Map.entry("vodka_order_line.ol_shipmode", 1),
            // Map.entry("vodka_order_line.ol_quantity", 1),
            // Map.entry("vodka_order_line.ol_shipinstruct", 2),
            // Map.entry("vodka_order_line.ol_discount", 1),

            // /* ---------- customer / order ---------- */
            // Map.entry("vodka_customer.c_phone", 2),
            // Map.entry("vodka_customer.c_balance", 0),
            // Map.entry("vodka_customer.c_nationkey", 2),
            // Map.entry("vodka_customer.c_mktsegment", 2),
            // Map.entry("vodka_order.o_comment", 0),

            // /* ---------- supplier / stock ---------- */
            // Map.entry("vodka_supplier.s_nationkey", 2),
            // Map.entry("vodka_stock.s_supplycost", 1),

            // /* ---------- item ---------- */
            // Map.entry("vodka_item.i_size", 1),
            // Map.entry("vodka_item.i_brand", 1),
            // Map.entry("vodka_item.i_container", 0),
            // Map.entry("vodka_item.i_type", 2),

            // /* ---------- nation / region ---------- */
            // Map.entry("vodka_nation.n_name", 4) // region.r_name 已按需求移除

            // 0 - 3
            // /* ---------- order-line ---------- */
            Map.entry("vodka_order_line.ol_returnflag", 1),
            Map.entry("vodka_order_line.ol_shipmode", 1),
            Map.entry("vodka_order_line.ol_quantity", 3),
            Map.entry("vodka_order_line.ol_shipinstruct", 2),
            Map.entry("vodka_order_line.ol_discount", 2),

            /* ---------- customer / order ---------- */
            Map.entry("vodka_customer.c_phone", 3),
            Map.entry("vodka_customer.c_balance", 0),
            Map.entry("vodka_customer.c_nationkey", 3),
            Map.entry("vodka_customer.c_mktsegment", 3),
            Map.entry("vodka_order.o_comment", 0),

            /* ---------- supplier / stock ---------- */
            Map.entry("vodka_supplier.s_nationkey", 1),
            Map.entry("vodka_stock.s_supplycost", 1),

            /* ---------- item ---------- */
            Map.entry("vodka_item.i_size", 2),
            Map.entry("vodka_item.i_brand", 2),
            Map.entry("vodka_item.i_container", 0),
            Map.entry("vodka_item.i_type", 2),

            /* ---------- nation / region ---------- */
            Map.entry("vodka_nation.n_name", 4) // region.r_name 已按需求移除
    );

    /** 禁止实例化 */
    private ZipfConfig() {
    }
}
