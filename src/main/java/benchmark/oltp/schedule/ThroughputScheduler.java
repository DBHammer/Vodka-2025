package benchmark.oltp.schedule;

import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

import lombok.Getter;
import lombok.Setter;

/**
 * 随机吞吐调度器
 * — tps ∈ [1000,6000] 均匀伪随机
 * — Pattern ∈ {DEFAULT,BALANCED,READ_PEAK} 随机打乱
 * — 段时长：截断指数(λ)≥60s，然后整体缩放到 runMin
 */
@Getter
@Setter
public final class ThroughputScheduler {

    /* ---------- txn-mix ---------- */
    public enum Pattern {
        DEFAULT, BALANCED, READ_PEAK
    }

    private final boolean isBurst;

    public static final class Weights {
        public final int newOrder, payment, delivery, receiveGoods, orderStatus, stockLevel;

        Weights(int n, int p, int d, int r, int o, int s) {
            newOrder = n;
            payment = p;
            delivery = d;
            receiveGoods = r;
            orderStatus = o;
            stockLevel = s;
        }
    }

    private static final Map<Pattern, Weights> W = Map.of(
            Pattern.DEFAULT, new Weights(44, 42, 4, 2, 4, 4),
            Pattern.BALANCED, new Weights(21, 19, 6, 4, 25, 25),
            Pattern.READ_PEAK, new Weights(4, 3, 2, 1, 45, 45));

    public static Weights weights(Pattern p) {
        return W.get(p);
    }

    /* ---------- segment bean ---------- */
    public static final class Segment {
        public final int tps;
        public final Pattern pattern;
        public final long sec;

        Segment(int t, Pattern p, long s) {
            tps = t;
            pattern = p;
            sec = s;
        }
    }

    private final List<Segment> segs = new ArrayList<>();

    /** 读取只读列表 */
    public List<Segment> segments() {
        return Collections.unmodifiableList(segs);
    }

    /**
     * @param runMin     实验总时长（分钟）
     * @param segmentCnt 段数 (建议 6)
     * @param seed       伪随机种子
     * @param meanSec    截断指数分布的平均秒数（如 150）
     */
    public ThroughputScheduler(int runMin, int segmentCnt, long seed,
            double meanSec, boolean isBurst) {
        this.isBurst = isBurst;
        Random r = new Random(seed);
        Random r2 = new Random(1234);

        /* ====== 1) 全随机 TPS (500-5000) ====== */
        final int MIN = 500;
        final int MAX = 5000;
        int[] tps = new int[segmentCnt];
        for (int i = 0; i < segmentCnt; i++) {
            tps[i] = r2.nextInt(MAX - MIN + 1) + MIN;
        }

        /* ====== 2) 模式随机/轮转 ====== */
        List<Pattern> patList = new ArrayList<>();
        Pattern[] cycle = { Pattern.DEFAULT, Pattern.BALANCED, Pattern.READ_PEAK };
        if (segmentCnt >= 6) {
            patList.add(cycle[0]);
            patList.add(cycle[1]);
            patList.add(cycle[2]);
            patList.add(cycle[0]);
            patList.add(cycle[2]);
            patList.add(cycle[1]);
        } else {
            for (int i = 0; i < segmentCnt; i++)
                patList.add(cycle[i % cycle.length]);
        }
        /* ====== 3) 分配秒数 ====== */
        long[] secs = allocSeconds(runMin * 60L, segmentCnt, r, meanSec);

        /* ====== 4) 构造初始段列表 ====== */
        List<Segment> tmp = new ArrayList<>(segmentCnt);
        for (int i = 0; i < segmentCnt; i++) {
            tmp.add(new Segment(tps[i], patList.get(i), secs[i]));
        }

        /* ====== 5) 可选：插入突发 ====== */
        // if (isBurst) {
        // final int BURST_TPS = 6500;
        // for (Segment s : tmp) {
        // long burstSec = 30 + r.nextInt(30);
        // if (s.sec <= burstSec) {
        // segs.add(s);
        // continue;
        // }
        // long remain = s.sec - burstSec;
        // long pre = (remain == 0) ? 0 : r.nextLong(remain + 1);
        // long post = remain - pre;
        // if (pre > 0)
        // segs.add(new Segment(s.tps, s.pattern, pre));
        // segs.add(new Segment(BURST_TPS, s.pattern, burstSec));
        // if (post > 0)
        // segs.add(new Segment(s.tps, s.pattern, post));
        // }
        // } else {
        // segs.addAll(tmp);
        // }
        if (isBurst) {
            Random burstRand = new Random(seed ^ 111);
            for (Segment s : tmp) {
                final int BURST_TPS = ThreadLocalRandom.current()
                    .nextInt(6000, 6701);
                int burstNum = burstRand.nextInt(0, 4); // 0,1,2
                long remain = s.sec;

                if (burstNum == 0 || remain < 30) {
                    segs.add(s);
                    continue;
                }

                // 随机生成 burstNum 个 burst 时长
                long[] burstLens = new long[burstNum];
                for (int i = 0; i < burstNum; i++) {
                    long burstSec = Math.min(
                            remain - (burstNum - i - 1) * 10,
                            10 + burstRand.nextInt(11) // [10,60]
                    );
                    burstLens[i] = burstSec;
                    remain -= burstSec;
                }

                // 将剩余平均分到 normLens
                long[] normLens = new long[burstNum + 1];
                long base = remain / (burstNum + 1);
                Arrays.fill(normLens, base);
                long extra = remain - base * (burstNum + 1);
                for (int i = 0; i < extra; i++)
                    normLens[i]++;

                // 交错加入“正常段”和“burst 段”，并保持 pattern=s.pattern
                for (int i = 0; i < burstNum; i++) {
                    if (normLens[i] > 0) {
                        segs.add(new Segment(s.tps, s.pattern, normLens[i]));
                    }
                    if (burstLens[i] > 0) {
                        segs.add(new Segment(BURST_TPS, s.pattern, burstLens[i]));
                    }
                }
                // 最后一段正常速度
                if (normLens[burstNum] > 0) {
                    segs.add(new Segment(s.tps, s.pattern, normLens[burstNum]));
                }
            }
        } else {
            segs.addAll(tmp);
        }
    }

    /* truncated-Exp ≥60s => scale to total */
    /**
     * 为每一段随机分配时长：
     * - 每段至少 MIN_SEC 秒（这里设为 120s）
     * - 再加一个 Poisson(meanPois) 的波动
     * - 最后一段再补齐，保证总和正好 = total
     */
    /**
     * 用 Poisson(λ) 采样 extras，再按比例缩放到 rem = total - MIN_SEC*n，
     * 最终 out[i] = MIN_SEC + extrasScaled[i]，且 ∑out = total。
     *
     * @param total  总秒数
     * @param n      段数
     * @param rnd    随机数生成器
     * @param lambda Poisson 分布的 λ（均值、方差都为 λ；λ 越大，波动越强）
     */
    private static long[] allocSeconds(long total, int n, Random rnd, double lambda) {
        final long MIN_SEC = 120; // 每段至少 120s
        long rem = total - MIN_SEC * n; // 除去“底线”后的剩余秒数

        // 1) 按 Poisson(lambda) 采样出 n 个 extras
        long[] extras = new long[n];
        long sumExtras = 0;
        for (int i = 0; i < n; i++) {
            extras[i] = nextPoisson(rnd, lambda);
            sumExtras += extras[i];
        }

        // 2) 将 extras 按比例映射到 [0, rem] 上
        long[] out = new long[n];
        long acc = 0;
        for (int i = 0; i < n; i++) {
            // 避免 sumExtras=0 的情况
            long extraScaled = sumExtras > 0
                    ? Math.round((double) extras[i] / sumExtras * rem)
                    : rem / n;
            out[i] = MIN_SEC + extraScaled;
            acc += out[i];
        }

        // 3) 四舍五入误差累到最后一段
        out[n - 1] += total - acc;
        return out;
    }

    /** Knuth 算法采样 Poisson */
    private static long nextPoisson(Random r, double lambda) {
        double L = Math.exp(-lambda);
        double p = 1.0;
        int k = 0;
        do {
            k++;
            p *= r.nextDouble();
        } while (p > L);
        return k - 1;
    }

    /* ----- tiny demo ----- */
    public static void main(String[] args) {
        ThroughputScheduler sch = new ThroughputScheduler(
                25, // 总 25 分钟
                6, // 3 段
                42, // 随机种子
                10, // 平均 120 s
                true // ← 开启突发
        );

        long sum = 0;
        for (ThroughputScheduler.Segment s : sch.segments()) {
            System.out.printf("%3d s | %4d tps | %s%n", s.sec, s.tps, s.pattern);
            sum += s.sec;
        }
        System.out.println("total secs = " + sum);
    }
}