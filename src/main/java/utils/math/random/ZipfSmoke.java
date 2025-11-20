package utils.math.random;

import java.util.Arrays;

public class ZipfSmoke {

    private static final String COL = "vodka_item.i_container";
    private static final int CARD = 40; // 5×8 组合
    private static final int N = 200_000;

    public static void main(String[] args) {
        for (int lvl = 0; lvl <= 4; lvl++) {
            ZipfConfig.SKEW.put(COL, lvl);
            int[] buckets = new int[CARD];
            for (int i = 0; i < N; i++)
                buckets[SkewRandom.next(COL, CARD)]++;

            System.out.printf("lvl=%d  head%%=%5.1f%%  tail%%=%4.1f%%  uniq=%d%n",
                    lvl,
                    buckets[0] * 100.0 / N,
                    buckets[CARD - 1] * 100.0 / N,
                    Arrays.stream(buckets).filter(x -> x > 0).count());
        }
    }
}
