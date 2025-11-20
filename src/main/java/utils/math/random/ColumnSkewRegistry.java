package utils.math.random;

import java.util.*;

/**
 * 1️⃣ 维护 “表.列 ➜ skew(z)” 的映射（支持多种模式）
 * 2️⃣ 提供按列名获取 z 的接口
 * 3️⃣ 演示如何在生成器里调用
 */
public final class ColumnSkewRegistry {

    /** 列唯一标识：建议 TableName.ColumnName 全小写 */
    public record ColumnKey(String table, String column) {
        public static ColumnKey of(String t, String c) {
            return new ColumnKey(t.toLowerCase(), c.toLowerCase());
        }
    }

    /** 可选 skew 模式：控制 z 值的采样集合大小 */
    public enum SkewMode {
        TWO(1), THREE(2), FOUR(3), FIVE(4); // max z 值，+1 是总数
        final int maxZ;
        SkewMode(int maxZ) { this.maxZ = maxZ; }
    }

    /** 保存 z 值（0‥4） */
    private static final Map<ColumnKey, Integer> SKEW_MAP = new HashMap<>();
    private static final Random RAND = new Random(38274);  // 固定种子，便于复现
    private static final double ZERO_PROB = 0.10;           // z=0 的概率

    /* ------------ 1. 初始化：为每列分配 z ------------ */
    static {
        // 默认使用 FIVE 模式（即 z ∈ {0,1,2,3,4}）
        register("vodka_orderline", "ol_returnflag");
        register("vodka_orderline", "ol_shipmode");
        register("vodka_orderline", "ol_discount");
        register("vodka_orderline", "ol_quantity");
        register("vodka_orderline", "ol_shipinstruct");

        register("vodka_item", "i_size");
        register("vodka_item", "i_type");
        register("vodka_item", "i_brand");
        register("vodka_item", "i_container");

        register("vodka_order", "o_comment", 0); // 固定为均匀分布
        register("vodka_customer", "c_mktsegment");
        register("vodka_customer", "c_phone");
        register("vodka_customer", "c_balance", 0);
        register("vodka_customer", "n_nationkey");

        register("vodka_stock", "s_supplycost");
        register("vodka_supplier", "s_nationkey");
    }

    /** 默认使用 SkewMode.FIVE（0~4） */
    private static void register(String table, String column) {
        register(table, column, SkewMode.FOUR);
    }

    /** 使用指定 skew 模式采样 z */
    private static void register(String table, String column, SkewMode mode) {
        int z;
        if (RAND.nextDouble() < ZERO_PROB) {
            z = 0;
        } else {
            z = 1 + RAND.nextInt(mode.maxZ);  // 均匀采样 1 ~ maxZ
        }
        register(table, column, z);
    }

    /** 手动指定某列的 z 值（例如强制为均匀） */
    private static void register(String table, String column, int z) {
        SKEW_MAP.put(ColumnKey.of(table, column), z);
    }

    /* ------------ 2. 对外接口 ------------ */

    /** 获取列的 z 值，若未注册则默认返回 0（即均匀） */
    public static int z(String table, String column) {
        return SKEW_MAP.getOrDefault(ColumnKey.of(table, column), 0);
    }

    /** 方便调试：打印全部分配结果 */
    public static void dump() {
        System.out.println("Table.Column -> z");
        SKEW_MAP.forEach((k, v) -> System.out.printf("%s.%s -> %d%n", k.table(), k.column(), v));
    }

    /* ------------ 3. 用例 Demo ------------ */
    public static void main(String[] args) {
        dump();

        // 示例：获取某列的 skew 值
        int z = z("vodka_orderline", "ol_quantity");
        System.out.printf("%n示例：vodka_orderline.ol_quantity 的 skew z = %d%n", z);
    }
}