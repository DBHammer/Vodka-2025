package benchmark.synchronize.tasks;

import java.sql.Connection;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.math3.util.Pair;

import static benchmark.oltp.OLTPClient.*;
import benchmark.synchronize.components.HTAPCheckInfo;

/**
 * 单查询 + 粘住窗口（占位）版 FreshnessTask2（只做行级 GC）
 *
 * - 每轮基于“传入的 currentTime”构造 [currentTime-W, currentTime]。
 * - beginCycleSticky 只是占位（不做窗口GC/推进）；GC 由 FreshnessExecutor 决定。
 * - computeFreshnessSingle 内部：AP 扫描 → 计算；（GC 仅在 latest&complete 时由执行器异步触发）
 */
public class FreshnessTask2 extends Task {

    private static final AtomicLong maxFreshness = new AtomicLong(0);

    /** 业务窗口 W（秒），来自配置 */
    private final long freshnessTimeBound;

    /** 作为右边界使用的 currentTime（建议来自 DB NOW() 或统一时间源） */
    private final Timestamp rightBoundaryTs;

    public FreshnessTask2(int dbType, HTAPCheckInfo htapCheckInfo, Timestamp currentTime) {
        this.freshnessTimeBound = htapCheckInfo.htapCheckFreshnessDataBound;
        this.rightBoundaryTs = currentTime; // 用它当 rTs
    }

    @Override
    public TaskResult runTask(ArrayList<Connection> conns, int threadId) {
        Connection apConnection = conns.get(0);

        System.out.printf("Remain #%d Real-time query%n", htapCheckQueryNumber.decrementAndGet());

        long startTime = System.currentTimeMillis();
        long Wms = freshnessTimeBound * 1000L;

        // 用传入的 currentTime 作为右边界；若意外为 null，兜底用系统时间
        Timestamp rTs = (rightBoundaryTs != null) ? rightBoundaryTs : new Timestamp(System.currentTimeMillis());
        Timestamp lTs = new Timestamp(rTs.getTime() - Wms);

        FreshnessExecutor.CycleStart cycle = FreshnessExecutor.beginCycleSticky(lTs, rTs);

        long vodkaFreshness = 0L;
        long computeMs = 0L;
        try {
            long[] res = FreshnessExecutor.computeFreshnessSingle(apConnection, cycle.lTs, cycle.rTs, startTime);
            // vodkaFreshness = res[0]; // 如需打印实际 freshness 再打开
            computeMs = res[2];
        } catch (Exception e) {
            e.printStackTrace();
        }

        long end = System.currentTimeMillis();
        long totalMs = end - startTime;
        endTime = startTime + totalMs;

        long gapSinceLast = 0; // 如需记录上次触发间隔，可在外部维护
        maxFreshness.updateAndGet(x -> Math.max(x, vodkaFreshness));

        System.out.printf(
            ">>> Freshness(AP)=%d ms | totalLatency=%d ms | gap=%d ms (windowGC=%d, compute=%d) | window=[%s, %s] | %s%n",
            vodkaFreshness, totalMs, gapSinceLast, 0L, computeMs, cycle.lTs, cycle.rTs, FreshnessExecutor.traceStats()
        );

        // 记录历史
        freshnessHistory.add(new Pair<>(0L, vodkaFreshness));
        if (htapCheckQueryNumber.get() <= 0) {
            printFreshnessHistory();
        }

        return new TaskResult(
                taskType,
                txnCompleteTime,
                gapTime,
                startTime,
                endTime,
                0,              // tryNum
                true,           // pass
                false,          // isApConnErr
                computeMs,
                vodkaFreshness,
                0,              // vodkaFreshness2（未用）
                0,
                0
        );
    }

    private void printFreshnessHistory() {
        System.out.println("Complete Freshness History, Deviations, and Deviation Ratios:");
        for (Pair<Long, Long> results : freshnessHistory) {
            long hatFreshness = results.getFirst();
            long vodkaFreshness = results.getSecond();
            long deviation = Math.abs(hatFreshness - vodkaFreshness);
            double deviationRatio = vodkaFreshness != 0 ? (double) deviation / vodkaFreshness : 0;
            System.out.println("HATtrick Freshness: " + hatFreshness + "ms" +
                    ", Vodka Freshness: " + vodkaFreshness + "ms" +
                    ", Deviation: " + deviation +
                    ", Deviation Ratio: " + String.format("%.2f", deviationRatio));
        }
        try { signalTerminalsRequestEnd(false); } catch (Throwable e) { e.printStackTrace(); }
    }
}