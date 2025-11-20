package benchmark.synchronize.tasks;

import java.sql.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAccumulator;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;

/**
 * 单查询 + 粘住窗口（sticky）
 * 仅当窗口判定为 latest&complete 时，才：
 * - 异步触发行级 GC（按 AP 可见版本剪枝，且可选“ver==MAX_VERSION 整行删”）
 * - 异步触发窗口 GC（清理 dbTsMs < lTs）
 *
 * 其它要点：
 * - 修复并发下 firstEntry()/lastEntry() 可能为 null 的 NPE。
 * - 提供多项可调开关与统计信息便于观测。
 */
public final class FreshnessExecutor {

    private FreshnessExecutor() {
    }

    /* ─────────── 常量/线程池 ─────────── */

    private static final byte IDX_DELIVERY = 0;
    private static final byte IDX_RECEIPT = 1;

    // 追踪表
    private static final Map<Long, Row> TRACE = new ConcurrentHashMap<>(1 << 18);

    // 计算线程池
    private static final ForkJoinPool FJP = new ForkJoinPool(Math.max(2, Runtime.getRuntime().availableProcessors()));

    // 行级 GC（异步）
    private static final ExecutorService ROW_GC_EXEC = Executors.newSingleThreadExecutor(r -> {
        Thread t = new Thread(r, "FreshnessExecutor-ROW-GC");
        t.setDaemon(true);
        return t;
    });

    // 窗口 GC（异步）
    private static final ExecutorService WINDOW_GC_EXEC = Executors.newSingleThreadExecutor(r -> {
        Thread t = new Thread(r, "FreshnessExecutor-WINDOW-GC");
        t.setDaemon(true);
        return t;
    });

    /* ─────────── 行为开关/参数 ─────────── */

    /** AP 判定截至 rTs 已最新时是否整行清理 */
    public static volatile boolean PURGE_ROW_IF_UPTODATE = true;

    /** AP 可见版本达到 MAX_VISIBLE_VERSION 时是否整行清理 */
    public static volatile boolean PURGE_ROW_IF_VERSION_REACHED = true;
    public static volatile int MAX_VISIBLE_VERSION = 3;

    /** 仅日志参数（保留接口，当前逻辑未直接使用） */
    public static volatile long MIN_DELAY_BOUND_MS = 1000L;
    public static volatile double ALPHA = 1.0;
    public static volatile long MAX_HORIZON_MS = Long.MAX_VALUE;

    public static void setGcPolicy(long minDelayMs, double alpha, long maxHorizonMs) {
        if (minDelayMs >= 0)
            MIN_DELAY_BOUND_MS = minDelayMs;
        if (alpha > 0)
            ALPHA = alpha;
        if (maxHorizonMs > 0)
            MAX_HORIZON_MS = maxHorizonMs;
        System.out.printf("[GC-Policy] L_min=%dms alpha=%.3f H_max=%dms%n",
                MIN_DELAY_BOUND_MS, ALPHA, MAX_HORIZON_MS);
    }

    /* ─────────── 粘窗状态 ─────────── */

    private static final class WindowState {
        final Timestamp lTs, rTs;
        volatile boolean upToDate;

        WindowState(Timestamp l, Timestamp r, boolean up) {
            this.lTs = l;
            this.rTs = r;
            this.upToDate = up;
        }
    }

    private static final AtomicReference<WindowState> LAST_WINDOW = new AtomicReference<>(null);
    private static final ReentrantLock WINDOW_LOCK = new ReentrantLock();

    public static final class CycleStart {
        public final Timestamp lTs, rTs;
        public final long windowGcMs, rowGcMs;

        CycleStart(Timestamp l, Timestamp r, long wgc, long rgc) {
            this.lTs = l;
            this.rTs = r;
            this.windowGcMs = wgc;
            this.rowGcMs = rgc;
        }
    }

    // GC 统计
    private static final AtomicLong LAST_WINDOW_GC_MS = new AtomicLong(0L);
    private static final AtomicLong WINDOW_GC_COUNT = new AtomicLong(0L);
    private static final AtomicBoolean WINDOW_GC_RUNNING = new AtomicBoolean(false);

    private static final AtomicLong LAST_ROW_GC_MS = new AtomicLong(0L);
    private static final AtomicLong ROW_GC_COUNT = new AtomicLong(0L);
    private static final AtomicBoolean ROW_GC_RUNNING = new AtomicBoolean(false);

    /* ─────────── 结构 ─────────── */

    static final class TimePair {
        long commitMs, dbTsMs;

        TimePair(long c, long d) {
            commitMs = c;
            dbTsMs = d;
        }
    }

    static final class Row {
        @SuppressWarnings("unchecked")
        final NavigableMap<Integer, TimePair>[] cols = new NavigableMap[2]; // 0=delivery,1=receipt
    }

    static final class VisibleRow {
        final long pk;
        final int ver;
        final long apTsMs; // 预留

        VisibleRow(long pk, int ver) {
            this(pk, ver, 0L);
        }

        VisibleRow(long pk, int ver, long apTs) {
            this.pk = pk;
            this.ver = ver;
            this.apTsMs = apTs;
        }
    }

    // 为异步 GC 拍快照的轻量结构
    private static final class VisItem {
        final long pk;
        final int ver;

        VisItem(long pk, int ver) {
            this.pk = pk;
            this.ver = ver;
        }
    }

    /* ─────────── 工具 ─────────── */

    private static void logCost(String label, long startNs) {
        long ms = (System.nanoTime() - startNs) / 1_000_000;
        System.out.printf("[Profile] %-28s : %d ms%n", label, ms);
    }

    private static long packPk(int w, int d, int o) {
        return ((long) w << 32) | ((long) d << 16) | (o & 0xFFFFL);
    }

    /* ─────────── 写入追踪 ─────────── */

    public static void addTransaction(List<Integer> pk, String col, int version,
            Timestamp commitTime, Timestamp dbCurrentTs) {
        long rowKey = packPk(pk.get(0), pk.get(1), pk.get(2));
        long commitMs = commitTime.getTime();
        long dbTsMs = dbCurrentTs.getTime();

        Row row = TRACE.computeIfAbsent(rowKey, k -> new Row());
        byte idx = (col != null && col.length() > 3 && col.charAt(3) == 'd') ? IDX_DELIVERY : IDX_RECEIPT;
        byte other = (idx == IDX_DELIVERY) ? IDX_RECEIPT : IDX_DELIVERY;

        NavigableMap<Integer, TimePair> vm = row.cols[idx];
        if (vm == null)
            row.cols[idx] = vm = new ConcurrentSkipListMap<>();
        vm.put(version, new TimePair(commitMs, dbTsMs));

        NavigableMap<Integer, TimePair> om = row.cols[other];
        if (om == null)
            row.cols[other] = om = new ConcurrentSkipListMap<>();
        int prev = version - 1;
        long otherCommit = om.containsKey(prev) ? om.get(prev).commitMs
                : vm.containsKey(prev) ? vm.get(prev).commitMs : commitMs;
        om.putIfAbsent(version, new TimePair(otherCommit, dbTsMs));
    }

    /* ─────────── AP 扫描 ─────────── */

    public static Long2ObjectOpenHashMap<VisibleRow> fetchColumnVersions(Connection ap, Timestamp lTs, Timestamp rTs)
            throws SQLException {
        final String sql = "SELECT ol_w_id,ol_d_id,ol_o_id, access_version AS ver " +
                "FROM vodka_order_line WHERE current_ts BETWEEN ? AND ?";
        Long2ObjectOpenHashMap<VisibleRow> apMap = new Long2ObjectOpenHashMap<>(1_000_000, 0.9f);
        ap.setAutoCommit(false);
        long t0 = System.nanoTime();
        int rowCount = 0;
        try (PreparedStatement ps = ap.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
            ps.setFetchSize(20000);
            ps.setTimestamp(1, lTs);
            ps.setTimestamp(2, rTs);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    long pk = packPk(rs.getInt(1), rs.getInt(2), rs.getInt(3));
                    int ver = rs.getInt("ver");
                    apMap.put(pk, new VisibleRow(pk, ver));
                    rowCount++;
                }
            }
        }
        ap.commit();
        logCost("Acquire Result Set (Single)", t0);
        System.out.println("AP returned " + rowCount);
        return apMap;
    }

    /* ─────────── 粘窗生命周期 ─────────── */

    public static CycleStart beginCycleSticky(Timestamp proposedLTs, Timestamp proposedRTs) {
        long lastWgc = LAST_WINDOW_GC_MS.get();
        long lastRgc = LAST_ROW_GC_MS.get();

        WindowState st = LAST_WINDOW.get();
        if (st != null && !st.upToDate) {
            System.out.printf("[Window] Reuse sticky window [%s, %s] (not latest&complete)%n", st.lTs, st.rTs);
            return new CycleStart(st.lTs, st.rTs, lastWgc, lastRgc);
        }

        WINDOW_LOCK.lock();
        try {
            st = LAST_WINDOW.get();
            if (st == null || st.upToDate) {
                WindowState nw = new WindowState(proposedLTs, proposedRTs, /* up */ false);
                LAST_WINDOW.set(nw);
                System.out.printf("[Window] Adopt new window [%s, %s]%n", proposedLTs, proposedRTs);
                return new CycleStart(proposedLTs, proposedRTs, lastWgc, lastRgc);
            } else {
                System.out.printf("[Window] Reuse sticky window [%s, %s] (not latest&complete)%n", st.lTs, st.rTs);
                return new CycleStart(st.lTs, st.rTs, lastWgc, lastRgc);
            }
        } finally {
            WINDOW_LOCK.unlock();
        }
    }

    /** 标记窗口最新&完全（不再做 final purge；改为由外部触发异步 Row-GC/Window-GC） */
    public static void endCycle(Timestamp lTs, Timestamp rTs, boolean latestAndComplete) {
        if (!latestAndComplete)
            return;
        WindowState st = LAST_WINDOW.get();
        if (st != null && st.lTs.equals(lTs) && st.rTs.equals(rTs) && !st.upToDate) {
            st.upToDate = true;
            System.out.println("[Window] Marked up-to-date (latest&complete).");
        }
    }

    /* ─────────── “最新且完全”相关工具 ─────────── */

    private static int latestVersionAtOrBefore(long pk, long rMs) {
        Row row = TRACE.get(pk);
        if (row == null)
            return -1;
        int max = -1;
        for (int i = 0; i < 2; i++) {
            NavigableMap<Integer, TimePair> vm = row.cols[i];
            if (vm == null || vm.isEmpty())
                continue;
            Map.Entry<Integer, TimePair> ent = vm.lastEntry();
            if (ent == null)
                continue; // 并发下保护
            // 逆序找 <= rMs 的最后一个
            for (Map.Entry<Integer, TimePair> e : vm.descendingMap().entrySet()) {
                TimePair tp = e.getValue();
                if (tp != null && tp.dbTsMs <= rMs) {
                    max = Math.max(max, e.getKey());
                    break;
                }
            }
        }
        return max;
    }

    private static boolean rowHasVersionInWindow(Row row, long lMs, long rMs) {
        if (row == null)
            return false;
        for (int i = 0; i < 2; i++) {
            NavigableMap<Integer, TimePair> vm = row.cols[i];
            if (vm == null || vm.isEmpty())
                continue;
            Map.Entry<Integer, TimePair> fe = vm.firstEntry();
            Map.Entry<Integer, TimePair> le = vm.lastEntry();
            if (fe == null || le == null)
                continue; // 并发保护
            long min = fe.getValue().dbTsMs;
            long max = le.getValue().dbTsMs;
            if (max < lMs || min > rMs)
                continue;
            for (Map.Entry<Integer, TimePair> e : vm.entrySet()) {
                TimePair tp = e.getValue();
                if (tp == null)
                    continue;
                long t = tp.dbTsMs;
                if (t < lMs)
                    continue;
                if (t > rMs)
                    break;
                return true;
            }
        }
        return false;
    }

    // 截至 rMs 的“可见版本”（只看单行）
    private static int latestVersionAtOrBefore(Row row, long rMs) {
        if (row == null)
            return -1;
        int max = -1;
        for (int i = 0; i < 2; i++) {
            NavigableMap<Integer, TimePair> vm = row.cols[i];
            if (vm == null || vm.isEmpty())
                continue;
            for (Map.Entry<Integer, TimePair> e : vm.descendingMap().entrySet()) {
                TimePair tp = e.getValue();
                if (tp != null && tp.dbTsMs <= rMs) {
                    max = Math.max(max, e.getKey());
                    break;
                }
            }
        }
        return max;
    }

    // 求指定版本在该行的 dbTsMs（两列可能都有，取较大值更保守）
    private static long dbTsOfVersion(Row row, int ver) {
        long ts = Long.MIN_VALUE;
        for (int i = 0; i < 2; i++) {
            NavigableMap<Integer, TimePair> vm = row.cols[i];
            if (vm == null)
                continue;
            TimePair tp = vm.get(ver);
            if (tp != null)
                ts = Math.max(ts, tp.dbTsMs);
        }
        return ts;
    }

    // 求该行“最后一笔写”的 dbTsMs（判断是否 rTs 之后还有写）
    private static long lastDbTs(Row row) {
        long rowMax = Long.MIN_VALUE;
        for (int i = 0; i < 2; i++) {
            NavigableMap<Integer, TimePair> vm = row.cols[i];
            if (vm == null || vm.isEmpty())
                continue;
            Map.Entry<Integer, TimePair> le = vm.lastEntry();
            if (le != null && le.getValue() != null) {
                rowMax = Math.max(rowMax, le.getValue().dbTsMs);
            }
        }
        return rowMax;
    }

    public static boolean isApLatestAndComplete(Long2ObjectOpenHashMap<VisibleRow> apMap,
            Timestamp lTs, Timestamp rTs) {
        final long lMs = lTs.getTime(), rMs = rTs.getTime();

        int should = 0, missing = 0, hit = 0, moved = 0;

        for (Map.Entry<Long, Row> ent : TRACE.entrySet()) {
            long pk = ent.getKey();
            Row row = ent.getValue();
            if (row == null)
                continue;

            // 1) 截至 rTs 的可见版本
            int visVer = latestVersionAtOrBefore(row, rMs);
            if (visVer < 0)
                continue;

            long visDb = dbTsOfVersion(row, visVer);
            if (visDb < lMs || visDb > rMs) {
                // 可见版本不在窗口里，不参与
                continue;
            }

            // 2) 如果这行在 rTs 之后还有写（lastDbTs > rMs），说明“已移出窗口”，
            // 你的 AP SQL 按 current_ts 过滤本就不会返回它 —— 不应计入 should/missing
            long rowLast = lastDbTs(row);
            if (rowLast > rMs) {
                moved++;
                continue;
            }

            // 3) 真正需要 AP 返回的样本
            should++;

            VisibleRow ap = apMap.get(pk);
            if (ap != null && ap.ver == visVer) {
                hit++;
            } else {
                missing++;
            }
        }

        if (should == 0) {
            System.out.println(
                    "[Check] Window has no tracked *visible* keys that still stay within rTs (should=0) — accept.");
            return true;
        }
        if (missing > 0) {
            System.out.printf("[Check] Incomplete: should=%d, missing=%d, hitOnTrace=%d, movedOut=%d%n",
                    should, missing, hit, moved);
            return false;
        }

        System.out.printf("[Check] Complete: should=%d, missing=0, hitOnTrace=%d, movedOut=%d%n",
                should, hit, moved);
        return true;
    }
    /* ─────────── Freshness 计算（不做同步 GC） ─────────── */

    public static long[] computeFreshnessSingle(Connection apConn,
            Timestamp lTs, Timestamp rTs,
            long queryStartMs) throws Exception {
        Long2ObjectOpenHashMap<VisibleRow> apRows = fetchColumnVersions(apConn, lTs, rTs);

        boolean latestAndComplete = isApLatestAndComplete(apRows, lTs, rTs);
        endCycle(lTs, rTs, latestAndComplete);

        // 仅计算 freshness；不在此处做任何 GC 等待
        long t0 = System.currentTimeMillis();
        long apFresh = calcFreshnessAP(apRows, lTs, rTs, queryStartMs);
        long durMs = System.currentTimeMillis() - t0;

        // 只有 latest&complete 才触发异步 GC（行级 + 窗口）
        if (latestAndComplete) {
            // 行级 GC：用 AP 可见版本做剪枝（异步、拍快照）
            triggerRowGCAsync(apRows, rTs);
            // 窗口 GC：清理 < lTs 的历史（异步）
            scheduleWindowGCAsync(lTs);
        } else {
            System.out.println("[GC] skip (not latest&complete)");
        }

        return new long[] { apFresh, 0L, durMs };
    }

    public static long calcFreshnessAP(Long2ObjectOpenHashMap<VisibleRow> apMap,
            Timestamp lTs, Timestamp now, long qStart) {
        return calcFreshnessAP(apMap, lTs, now, qStart, lTs);
    }

    public static long calcFreshnessAP(Long2ObjectOpenHashMap<VisibleRow> apMap,
            Timestamp lTs, Timestamp now,
            long qStart, Timestamp unused) {
        final long lMs = lTs.getTime(), nMs = now.getTime();
        LongAccumulator max = new LongAccumulator(Long::max, 0);

        long t = System.nanoTime();
        FJP.submit(() -> TRACE.entrySet().stream().parallel().forEach(ent -> {
            long pk = ent.getKey();
            Row row = ent.getValue();

            long rowMin = Long.MAX_VALUE, rowMax = Long.MIN_VALUE;
            for (NavigableMap<Integer, TimePair> vm : row.cols) {
                if (vm == null || vm.isEmpty())
                    continue;
                Map.Entry<Integer, TimePair> fe = vm.firstEntry();
                Map.Entry<Integer, TimePair> le = vm.lastEntry();
                if (fe == null || le == null)
                    continue; // 并发保护
                TimePair feTp = fe.getValue(), leTp = le.getValue();
                if (feTp == null || leTp == null)
                    continue;
                rowMin = Math.min(rowMin, feTp.dbTsMs);
                rowMax = Math.max(rowMax, leTp.dbTsMs);
            }
            if (rowMax < lMs || rowMin > nMs)
                return;

            VisibleRow vr = apMap.get(pk);
            if (vr != null) {
                long local = 0;
                for (NavigableMap<Integer, TimePair> vm : row.cols) {
                    if (vm == null || vm.isEmpty())
                        continue;
                    TimePair vis = vm.get(vr.ver);
                    if (vis == null)
                        continue;
                    Map.Entry<Integer, TimePair> le = vm.lastEntry();
                    if (le == null || le.getValue() == null)
                        continue; // 并发保护
                    long diff = le.getValue().commitMs - vis.commitMs;
                    if (diff > local)
                        local = diff;
                }
                if (local > 0)
                    max.accumulate(local);
                return;
            }

            long earliestCommit = Long.MAX_VALUE;
            for (NavigableMap<Integer, TimePair> vm : row.cols) {
                if (vm == null || vm.isEmpty())
                    continue;
                for (Map.Entry<Integer, TimePair> e : vm.entrySet()) {
                    TimePair tp = e.getValue();
                    if (tp == null)
                        continue;
                    long tDb = tp.dbTsMs;
                    if (tDb < lMs)
                        continue;
                    if (tDb > nMs)
                        break;
                    earliestCommit = Math.min(earliestCommit, tp.commitMs);
                }
            }
            if (earliestCommit != Long.MAX_VALUE) {
                max.accumulate(qStart - earliestCommit);
            }
        })).join();
        logCost("Step-2 scan TRACE", t);

        long result = max.get();
        System.out.printf(">>> maxFresh = %d ms%n", result);
        return result;
    }

    /* ─────────── 行级 GC（异步，仅在 latest&complete 后触发） ─────────── */

    private static List<VisItem> snapshotVisible(Long2ObjectOpenHashMap<VisibleRow> apMap) {
        if (apMap.isEmpty())
            return Collections.emptyList();
        List<VisItem> list = new ArrayList<>(apMap.size());
        for (Long2ObjectMap.Entry<VisibleRow> e : apMap.long2ObjectEntrySet()) {
            list.add(new VisItem(e.getLongKey(), e.getValue().ver));
        }
        return list;
    }

    private static int latestOverallVersion(long pk) {
        Row row = TRACE.get(pk);
        if (row == null)
            return -1;
        int max = -1;
        for (int i = 0; i < 2; i++) {
            NavigableMap<Integer, TimePair> vm = row.cols[i];
            if (vm != null && !vm.isEmpty()) {
                Integer lastK = vm.lastKey();
                if (lastK != null)
                    max = Math.max(max, lastK);
            }
        }
        return max;
    }

    private static boolean isRowUpToDateAsOf(long pk, int visibleVer, long rMs) {
        int latestAsOf = latestVersionAtOrBefore(pk, rMs);
        return latestAsOf <= visibleVer; // latestAsOf=-1 也视为可清理
    }

    private static void triggerRowGCAsync(Long2ObjectOpenHashMap<VisibleRow> apMap, Timestamp rTs) {
        final long rMs = rTs.getTime();
        final List<VisItem> visList = snapshotVisible(apMap);
        if (visList.isEmpty()) {
            System.out.println("[Row-GC] skip (empty snapshot)");
            return;
        }
        ROW_GC_EXEC.submit(() -> {
            ROW_GC_RUNNING.set(true);
            long t0 = System.nanoTime();
            long rowsVisited = 0, rowsPurged = 0, versionsRemoved = 0, purgedByMaxVer = 0, purgedUpToDate = 0;

            try {
                visList.parallelStream().forEach(v -> {
                    Row row = TRACE.get(v.pk);
                    if (row == null)
                        return;
                    // 先尝试“可见版本达到上限”整行删除
                    if (PURGE_ROW_IF_VERSION_REACHED) {
                        int overall = latestOverallVersion(v.pk);
                        if (overall >= 0 && v.ver >= MAX_VISIBLE_VERSION && overall <= MAX_VISIBLE_VERSION) {
                            if (TRACE.remove(v.pk) != null) {
                                // 统计
                                synchronized (FreshnessExecutor.class) {
                                    // 轻量同步用于计数
                                    // （也可以用 AtomicLong，但这里数量有限）
                                }
                            }
                            // 用局部累加器避免共享竞争
                            // 下面用 ThreadLocal 方案，最后汇总（简单起见，此处直接加 AtomicLong）
                        }
                    }
                });

                // 第二遍：常规剪枝 + “截至 rTs 已最新”整行删
                for (VisItem v : visList) {
                    Row row = TRACE.get(v.pk);
                    if (row == null)
                        continue;
                    rowsVisited++;

                    long removedHere = 0L;

                    // 删 < visibleVer 的旧版本
                    for (int i = 0; i < 2; i++) {
                        NavigableMap<Integer, TimePair> vm = row.cols[i];
                        if (vm == null || vm.isEmpty())
                            continue;
                        NavigableMap<Integer, TimePair> head = vm.headMap(v.ver, false);
                        int n = head.size();
                        if (n > 0) {
                            removedHere += n;
                            head.clear();
                        }
                    }
                    versionsRemoved += removedHere;

                    // 截至 rTs 已“最新” → 且行整体没有 >rTs 的版本 → 整行清理
                    if (PURGE_ROW_IF_UPTODATE && isRowUpToDateAsOf(v.pk, v.ver, rMs)) {
                        boolean anyAfterR = false;
                        for (int i = 0; i < 2; i++) {
                            NavigableMap<Integer, TimePair> vm = row.cols[i];
                            if (vm == null || vm.isEmpty())
                                continue;
                            Map.Entry<Integer, TimePair> le = vm.lastEntry();
                            if (le != null && le.getValue() != null && le.getValue().dbTsMs > rMs) {
                                anyAfterR = true;
                                break;
                            }
                        }
                        if (!anyAfterR) {
                            TRACE.remove(v.pk);
                            rowsPurged++;
                            purgedUpToDate++;
                            continue;
                        }
                    }

                    // 两列皆空则删行
                    boolean still = false;
                    for (int i = 0; i < 2; i++) {
                        NavigableMap<Integer, TimePair> vm = row.cols[i];
                        if (vm == null || vm.isEmpty())
                            row.cols[i] = null;
                        else
                            still = true;
                    }
                    if (!still) {
                        TRACE.remove(v.pk);
                        rowsPurged++;
                    }
                }
            } catch (Throwable t) {
                t.printStackTrace();
            } finally {
                long ms = (System.nanoTime() - t0) / 1_000_000;
                LAST_ROW_GC_MS.set(ms);
                ROW_GC_COUNT.incrementAndGet();
                ROW_GC_RUNNING.set(false);
                System.out.printf("[Row-GC] elapsed=%d ms, rowsVisited=%d, rowsPurged=%d, versionsRemoved=%d%n",
                        ms, rowsVisited, rowsPurged, versionsRemoved);
            }
        });
    }

    /* ─────────── 窗口 GC（异步，仅在 latest&complete 后触发） ─────────── */

    private static void scheduleWindowGCAsync(Timestamp cutoffTs) {
        final long cutoffMs = cutoffTs.getTime();
        WINDOW_GC_EXEC.submit(() -> {
            WINDOW_GC_RUNNING.set(true);
            long t0 = System.nanoTime();
            long rowsVisited = 0, rowsPurged = 0, versionsRemoved = 0;

            try {
                for (Map.Entry<Long, Row> ent : TRACE.entrySet()) {
                    long pk = ent.getKey();
                    Row row = ent.getValue();
                    if (row == null)
                        continue;
                    rowsVisited++;

                    boolean stillHasData = false;
                    long removedHere = 0L;

                    for (int i = 0; i < 2; i++) {
                        NavigableMap<Integer, TimePair> vm = row.cols[i];
                        if (vm == null || vm.isEmpty()) {
                            row.cols[i] = null;
                            continue;
                        }

                        Iterator<Map.Entry<Integer, TimePair>> it = vm.entrySet().iterator();
                        while (it.hasNext()) {
                            Map.Entry<Integer, TimePair> e = it.next();
                            TimePair tp = e.getValue();
                            if (tp == null)
                                continue;
                            if (tp.dbTsMs < cutoffMs) {
                                it.remove();
                                removedHere++;
                            }
                        }
                        if (vm.isEmpty())
                            row.cols[i] = null;
                        else
                            stillHasData = true;
                    }
                    versionsRemoved += removedHere;
                    if (!stillHasData) {
                        TRACE.remove(pk);
                        rowsPurged++;
                    }
                }
            } catch (Throwable t) {
                t.printStackTrace();
            } finally {
                long ms = (System.nanoTime() - t0) / 1_000_000;
                LAST_WINDOW_GC_MS.set(ms);
                WINDOW_GC_COUNT.incrementAndGet();
                WINDOW_GC_RUNNING.set(false);
                System.out.printf(
                        "[Window-GC] cutoff<%s>, rowsVisited=%d, rowsPurged=%d, versionsRemoved=%d, elapsed=%d ms, count=%d%n",
                        new Timestamp(cutoffMs), rowsVisited, rowsPurged, versionsRemoved, ms, WINDOW_GC_COUNT.get());
            }
        });
    }

    /* ─────────── 统计/监控 ─────────── */

    public static String traceStats() {
        long rows = 0, vers = 0, cols = 0;
        for (Map.Entry<Long, Row> ent : TRACE.entrySet()) {
            rows++;
            Row row = ent.getValue();
            for (int i = 0; i < 2; i++) {
                NavigableMap<Integer, TimePair> vm = row.cols[i];
                if (vm != null) {
                    cols++;
                    vers += vm.size();
                }
            }
        }
        return String.format(
                "TRACE{rows=%d, nonNullCols=%d, versions=%d, windowGC(ms,last)=%d, windowGC(count)=%d, windowGC(running)=%s, rowGC(ms,last)=%d, rowGC(count)=%d, rowGC(running)=%s}",
                rows, cols, vers,
                LAST_WINDOW_GC_MS.get(), WINDOW_GC_COUNT.get(), WINDOW_GC_RUNNING.get(),
                LAST_ROW_GC_MS.get(), ROW_GC_COUNT.get(), ROW_GC_RUNNING.get());
    }

    /** 仅用于监控 */
    public static Map<Long, Row> getTraceMap() {
        return TRACE;
    }
}