package utils.math.random;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;

/** 单个 (N, α) 的抽样器 —— 内部存 CDF，二分检索 */
final class ZipfSampler {
    private final int N;
    private final double[] cdf; // 累计概率，长度 N

    ZipfSampler(int N, int alphaIdx) {
        this.N = N;

        /* 1. 取 α */
        int alpha = ZipfConst.ALPHA[alphaIdx - 1];

        /* 2. 计算 ζ_N(α)：α=1 用调和数，其余用常量 */
        double zeta;
        if (alpha == 1) {
            zeta = 0.0;
            for (int k = 1; k <= N; k++)
                zeta += 1.0 / k; // H_N
        } else {
            zeta = ZipfConst.ZETA[alphaIdx - 1];
        }

        /* 3. 构造累积概率表 */
        cdf = new double[N];
        double acc = 0.0;
        for (int k = 1; k <= N; k++) {
            acc += 1.0 / Math.pow(k, alpha) / zeta;
            cdf[k - 1] = acc;
        }
        cdf[N - 1] = 1.0; // 纠正尾差
    }

    /** 返回 0‥N-1 */
    int next() {
        double u = ThreadLocalRandom.current().nextDouble();
        int lo = 0, hi = N - 1;
        while (lo < hi) {
            int mid = (lo + hi) >>> 1;
            if (u < cdf[mid])
                hi = mid;
            else
                lo = mid + 1;
        }
        return lo;
    }
}

/** ζ(α) 常量表 & α 值（与原 C 版保持一致） */
final class ZipfConst {
    private ZipfConst() {
    }

    static final int[] ALPHA = { 1, 2, 3, 4 };
    static final double[] ZETA = {
            0.0, // 占位；α=1 时不用
            Math.PI * Math.PI / 6.0, // ζ(2)
            1.2020569031595942, // ζ(3)
            Math.pow(Math.PI, 4) / 90.0 // ζ(4)
    };
}

/** 线程安全的 (N, αIdx) 级缓存 */
final class ZipfCache {
    private static final ConcurrentHashMap<Long, ZipfSampler> CACHE = new ConcurrentHashMap<>();

    static ZipfSampler get(int N, int alphaIdx) {
        long key = (((long) N) << 3) | alphaIdx;
        return CACHE.computeIfAbsent(key, k -> new ZipfSampler(N, alphaIdx));
    }
}