package utils.math.random;

import java.util.Arrays;
import java.util.concurrent.ThreadLocalRandom;

/**
 * 用这个类替代你之前到处散落的 rnd.nextInt(...)
 * (依旧保留 ThreadLocalRandom 均匀取样的能力)
 */
public final class SkewRandom {

    private SkewRandom() {}

    /**
     * 根据 ZipfConfig 中的档位，对 0‥cardinality-1 做取样
     * @param column      完整列名，用来查档位
     * @param cardinality 离散取值个数
     */
    public static int next(String column, int cardinality) {
        int lvl = ZipfConfig.SKEW.getOrDefault(column, 0); // 0‥4
        if (lvl == 0) {
            return ThreadLocalRandom.current().nextInt(cardinality);
        }
        // lvl=1‥4 => αIdx=1‥4
        return ZipfCache.get(cardinality, lvl).next();
    }

    /* ======= 仍然暴露一些均匀包装，方便平滑替换 ========= */
    public static int uniformInt(int low, int high) {          // [low, high)
        return ThreadLocalRandom.current().nextInt(low, high);
    }
    public static long uniformLong(long low, long high) {      // [low, high)
        return ThreadLocalRandom.current().nextLong(low, high);
    }
    public static double uniformDouble(double low, double high){
        return ThreadLocalRandom.current().nextDouble(low, high);
    }

}



