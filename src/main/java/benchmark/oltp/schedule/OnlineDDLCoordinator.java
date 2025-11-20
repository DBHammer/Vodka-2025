/* ── benchmark/oltp/schedule/OnlineDDLCoordinator.java ───────── */
package benchmark.oltp.schedule;

import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

/** 线程安全、抢占式的 Online-DDL 协调器 */
public class OnlineDDLCoordinator {
    /** 已完成的最大阶段；–1 表示“正在做” */
    private static final AtomicInteger DONE_STAGE = new AtomicInteger(-1);

    private OnlineDDLCoordinator() {
    }

    /** 只等待到 doneStage >= stageIdx，然后返回 */
    public static void waitForStage(int stageIdx) {
        while (DONE_STAGE.get() < stageIdx) {
            // 轻量等待，不消耗太多 CPU
            LockSupport.parkNanos(50000_000L); // 500ms
        }
    }

    public static void ensureStage(int stageIdx) {
        // —— 专门处理第 0 阶段 ——
        if (stageIdx == 0) {
            // 只有第一个进来的线程会把 DONE_STAGE 从 <0 设置到 0
            // 其余线程看到 DONE_STAGE >= 0 直接 return
            if (DONE_STAGE.compareAndSet(-1, 0)) {
                try {
                    System.out.println("[DDL] 线程 " +
                            Thread.currentThread().getName() +
                            " 正在执行 Stage 0 (Create PK)");
                    OnlineDDLManager.INSTANCE.runDDL(0);
                } catch (SQLException e) {
                    throw new RuntimeException("Stage 0 DDL 失败", e);
                }
            }
            return;
        }

        // —— 原有逻辑，用于 Stage 1 及以后 ——
        while (true) {
            int done = DONE_STAGE.get();
            if (done >= stageIdx)
                return; // 已做完，跳过
            if (done == -1) { // 理论上不会再进这里
                LockSupport.parkNanos(500_000L);
                continue;
            }
            if (DONE_STAGE.compareAndSet(done, -1)) {
                try {
                    OnlineDDLManager.INSTANCE.runDDL(stageIdx);
                } catch (SQLException e) {
                    // 恢复成“上一个 done”，否则后续就永远卡住
                    DONE_STAGE.set(done);
                    throw new RuntimeException("Stage " + stageIdx + " DDL 失败", e);
                } finally {
                    DONE_STAGE.set(stageIdx);
                }
                return;
            }
        }
    }
}