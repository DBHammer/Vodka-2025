package benchmark.synchronize.tasks;

import java.sql.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAccumulator;

import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;

/**
 * 全局版本提交时间追踪器（支持多列属性，主键已压缩为 long）
 *
 * key = packedPk (w<<32 | d<<16 | o)
 * value = Map< columnName,
 * NavigableMap<version, TimePair(commitMs, dbTsMs)> >
 */
public final class FreshnessExecutor2 {

    private static final byte IDX_DELIVERY = 0; // delivery 列的版本信息
    private static final byte IDX_RECEIPT = 1; // receipdate 列的版本信息

    // 全局 Trace：pk → row → col → <ver, (commit,db)>
    private static final Map<Long, Row> TRACE = new ConcurrentHashMap<>(1 << 18);

    private static final int CPU = Runtime.getRuntime().availableProcessors();
    private static final ForkJoinPool FJP = new ForkJoinPool(CPU);

    // 一个版本对应了提交时间和其在数据库的谓词时间（便于 GC）
    private static final class TimePair {
        long commitMs;
        long dbTsMs;

        TimePair(long c, long d) {
            commitMs = c;
            dbTsMs = d;
        }
    }

    // 行级数据结构：每一行（主键）对应两个列级 Trace，每个 Trace 追踪 <版本，时间信息>
    private static final class Row {
        // 0 = delivery, 1 = receipt
        final NavigableMap<Integer, TimePair>[] cols = new NavigableMap[2];
    }

    /** AP 可见行：主键、可见版本号、AP 当前_ts（可选） */
    static final class VisibleRow {
        final long pk;
        final int ver;
        final long apTsMs; // 可选：当前实现未使用

        VisibleRow(long pk, int ver, long apTsMs) {
            this.pk = pk;
            this.ver = ver;
            this.apTsMs = apTsMs;
        }

        VisibleRow(long pk, int ver) {
            this(pk, ver, 0L);
        }
    }

    /* ────────────────────────── 后台线程池 ────────────────────────────── */
    private static final ScheduledExecutorService GC_EXEC = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "FreshnessExecutor-GC");
        t.setDaemon(true);
        return t;
    });

    /* ────────────────── 计时工具 ────────────────── */
    private static void logCost(String label, long startNs) {
        long ms = (System.nanoTime() - startNs) / 1_000_000;
        System.out.printf("[Profile] %-24s : %d ms%n", label, ms);
    }

    /* ──────────────────────────── 辅助函数 ────────────────────────────── */
    /** 把三段 16-bit ID 打成 64-bit 主键 */
    private static long packPk(int w, int d, int o) {
        return ((long) w << 32) | ((long) d << 16) | (o & 0xFFFFL);
    }

    private static long packPk(List<Integer> pk) {
        return packPk(pk.get(0), pk.get(1), pk.get(2));
    }

    /** 反解 64-bit packedPk → int[]{ w , d , o } */
    private static int[] unpackPk(long pk) {
        int o = (int) (pk & 0xFFFFL);
        int d = (int) ((pk >>> 16) & 0xFFFFL);
        int w = (int) ((pk >>> 32) & 0xFFFFL);
        return new int[] { w, d, o };
    }

    // 维护本地的 Map 结构，增加事务写入的版本信息
    public static void addTransaction(
            List<Integer> pk, // [w,d,o]
            String columnName, // 被改列
            int version, // 版本号
            Timestamp commitTime,
            Timestamp dbCurrentTs) {
        long rowKey = packPk(pk); // 行主键
        long commitMs = commitTime.getTime();
        long dbTsMs = dbCurrentTs.getTime();

        /* 1⃣️ 拿到 / 创建 Row 对象 */
        Row row = TRACE.computeIfAbsent(rowKey, k -> new Row());

        /* 2⃣️ 根据列名确定 idx（0=delivery, 1=receipt） */
        byte idx = (columnName.charAt(3) == 'd') // ol_**d**elivery_d
                ? IDX_DELIVERY
                : IDX_RECEIPT;
        byte otherIdx = (idx == IDX_DELIVERY) ? IDX_RECEIPT : IDX_DELIVERY;

        /* 3⃣️ 写入本列版本表 */
        NavigableMap<Integer, TimePair> verMap = row.cols[idx] = row.cols[idx] == null
                ? new ConcurrentSkipListMap<>()
                : row.cols[idx];
        verMap.put(version, new TimePair(commitMs, dbTsMs));

        /* 4⃣️ 同步另一列版本表（插入“影子版本”以保证两列 version 对齐） */
        NavigableMap<Integer, TimePair> otherMap = row.cols[otherIdx] = row.cols[otherIdx] == null
                ? new ConcurrentSkipListMap<>()
                : row.cols[otherIdx];
        int prevVer = version - 1;
        long otherCommitMs = otherMap.containsKey(prevVer) ? otherMap.get(prevVer).commitMs
                : verMap.containsKey(prevVer) ? verMap.get(prevVer).commitMs
                        : commitMs;
        otherMap.putIfAbsent(version, new TimePair(otherCommitMs, dbTsMs));
    }

    /*
     * ═════════════════════════════ ① AP 单次全窗口扫描（旧接口，保留）
     * ═════════════════════════════
     */
    public static Long2ObjectOpenHashMap<VisibleRow> fetchColumnVersions(Connection ap, Timestamp lTs, Timestamp rTs)
            throws SQLException {
        String sql = """
                SELECT ol_w_id,ol_d_id,ol_o_id, ol_delivery_d, ol_receipdate, access_version AS ver
                FROM   vodka_order_line
                WHERE  current_ts between ? AND ?
                """;
        Long2ObjectOpenHashMap<VisibleRow> apMap = new Long2ObjectOpenHashMap<>(1_000_000, 0.9f);
        int rowCount = 0;
        ap.setAutoCommit(false);
        try (PreparedStatement ps = ap.prepareStatement(
                sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
            ps.setFetchSize(20_000);
            ps.setTimestamp(1, lTs);
            ps.setTimestamp(2, rTs);
            long time1 = System.nanoTime();
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    rowCount++;
                    long pk = packPk(rs.getInt(1), rs.getInt(2), rs.getInt(3));
                    int ver = rs.getInt("ver");
                    apMap.put(pk, new VisibleRow(pk, ver));
                }
            }
            logCost("Acquire Result Set (Single)", time1);
        }
        ap.commit();
        System.out.println("AP returned " + rowCount);
        return apMap;
    }

    /*
     * ═════════════════════════════ ② AP 分半扫描（旧的“合并到 sink”版本，保留）
     * ═════════════════════════════
     */
    public static Long2ObjectOpenHashMap<VisibleRow> fetchColumnVersions(
            Connection apLeft, Connection apRight, Timestamp lTs, Timestamp rTs) throws SQLException {

        final long lMs = lTs.getTime();
        final long rMs = rTs.getTime();
        final long midMs = lMs + ((rMs - lMs) >>> 1) + 3000; // 旧逻辑保留（有 3s 缝隙）
        final Timestamp mid = new Timestamp(midMs);

        // 1) 并发安全的汇聚桶（2 个写线程）
        ConcurrentHashMap<Long, VisibleRow> sink = new ConcurrentHashMap<>(1 << 20, 0.75f, 2);

        // 2) 两半并行直接写入 sink
        Callable<Integer> leftTask = () -> fetchRangeInto(apLeft, lTs, mid, /* rightOpen */ false, "LeftHalf", sink);
        Callable<Integer> rightTask = () -> fetchRangeInto(apRight, mid, rTs, /* rightOpen */ true, "RightHalf", sink);

        int leftCnt, rightCnt;
        if (apLeft == apRight) {
            try {
                leftCnt = leftTask.call();
                rightCnt = rightTask.call();
            } catch (Exception e) {
                throw new SQLException("Sequential half-scan failed", e);
            }
        } else {
            ExecutorService es = Executors.newFixedThreadPool(2);
            try {
                Future<Integer> f1 = es.submit(leftTask);
                Future<Integer> f2 = es.submit(rightTask);
                leftCnt = f1.get();
                rightCnt = f2.get();
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                throw new SQLException("Parallel half-scan interrupted", ie);
            } catch (ExecutionException ee) {
                throw new SQLException("Parallel half-scan failed", ee.getCause());
            } finally {
                es.shutdown();
            }
        }

        Long2ObjectOpenHashMap<VisibleRow> apMap = new Long2ObjectOpenHashMap<>(sink.size(), 0.9f);
        sink.forEach(apMap::put);
        System.out.println("AP returned " + apMap.size()
                + " (left=" + leftCnt + ", right=" + rightCnt + ")");
        return apMap;
    }

    /** 把一半窗口直接灌入并发安全的 sink（旧接口配套） */
    private static int fetchRangeInto(
            Connection conn, Timestamp from, Timestamp to,
            boolean rightOpen, String label,
            ConcurrentHashMap<Long, VisibleRow> sink) throws SQLException {

        final String sql = rightOpen
                ? """
                        SELECT ol_w_id,ol_d_id,ol_o_id, ol_delivery_d, ol_receipdate, access_version AS ver
                        FROM   vodka_order_line
                        WHERE  current_ts > ? AND current_ts <= ?
                        """
                : """
                        SELECT ol_w_id,ol_d_id,ol_o_id, ol_delivery_d, ol_receipdate, access_version AS ver
                        FROM   vodka_order_line
                        WHERE  current_ts >= ? AND current_ts < ?
                        """;

        int rowCount = 0;
        try (PreparedStatement ps = conn.prepareStatement(
                sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
            ps.setFetchSize(20_000);
            ps.setTimestamp(1, from);
            ps.setTimestamp(2, to);

            long t = System.nanoTime();
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    long pk = packPk(rs.getInt(1), rs.getInt(2), rs.getInt(3));
                    int ver = rs.getInt("ver");
                    sink.put(pk, new VisibleRow(pk, ver)); // 直接写入并发桶
                    rowCount++;
                }
            }
            logCost("Acquire Result Set (" + label + ")", t);
        }
        System.out.println(label + " returned " + rowCount);
        return rowCount;
    }

    /*
     * ═════════════════════════════ ③ 新：分半扫描（返回左右两半的 Map，不合并）
     * ═════════════════════════════
     */
    /** 两半窗口结果 */
    public static final class Halves {
        public final Long2ObjectOpenHashMap<VisibleRow> left;
        public final Long2ObjectOpenHashMap<VisibleRow> right;

        Halves(Long2ObjectOpenHashMap<VisibleRow> l, Long2ObjectOpenHashMap<VisibleRow> r) {
            this.left = l;
            this.right = r;
        }
    }

    /** 单次取某一半窗口，返回 map（严格半开/半闭，避免漏/重） */
    private static Long2ObjectOpenHashMap<VisibleRow> fetchOneRangeMap(
            Connection conn, Timestamp from, Timestamp to, boolean rightOpen, String label) throws SQLException {
        final String sql = rightOpen
                ? """
                        SELECT ol_w_id,ol_d_id,ol_o_id, ol_delivery_d, ol_receipdate, access_version AS ver
                        FROM   vodka_order_line
                        WHERE  current_ts > ? AND current_ts <= ?
                        """
                : """
                        SELECT ol_w_id,ol_d_id,ol_o_id, ol_delivery_d, ol_receipdate, access_version AS ver
                        FROM   vodka_order_line
                        WHERE  current_ts >= ? AND current_ts < ?
                        """;
        Long2ObjectOpenHashMap<VisibleRow> map = new Long2ObjectOpenHashMap<>(1_000_000, 0.9f);
        int rowCount = 0;
        long t = System.nanoTime();
        try (PreparedStatement ps = conn.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY,
                ResultSet.CONCUR_READ_ONLY)) {
            ps.setFetchSize(20_000);
            ps.setTimestamp(1, from);
            ps.setTimestamp(2, to);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    long pk = packPk(rs.getInt(1), rs.getInt(2), rs.getInt(3));
                    int ver = rs.getInt("ver");
                    map.put(pk, new VisibleRow(pk, ver));
                    rowCount++;
                }
            }
        }
        logCost("Acquire Result Set (" + label + ")", t);
        System.out.println(label + " returned " + rowCount);
        return map;
    }

    /** 分半取数（同连接则串行、不同连接则并行），不引入 3s 缝隙 */
    public static Halves fetchColumnVersionsSplit(
            Connection apLeft, Connection apRight, Timestamp lTs, Timestamp rTs) throws SQLException {

        final long lMs = lTs.getTime(), rMs = rTs.getTime();
        final long midMs = lMs + ((rMs - lMs) >>> 1) + 3000; // 不+常数偏移，避免漏扫
        final Timestamp mid = new Timestamp(midMs);

        if (apLeft == apRight) {
            Long2ObjectOpenHashMap<VisibleRow> left = fetchOneRangeMap(apLeft, lTs, mid, /* rightOpen */ false,
                    "LeftHalf");
            Long2ObjectOpenHashMap<VisibleRow> right = fetchOneRangeMap(apRight, mid, rTs, /* rightOpen */ true,
                    "RightHalf");
            return new Halves(left, right);
        } else {
            ExecutorService es = Executors.newFixedThreadPool(2);
            try {
                Future<Long2ObjectOpenHashMap<VisibleRow>> fL = es
                        .submit(() -> fetchOneRangeMap(apLeft, lTs, mid, false, "LeftHalf"));
                Future<Long2ObjectOpenHashMap<VisibleRow>> fR = es
                        .submit(() -> fetchOneRangeMap(apRight, mid, rTs, true, "RightHalf"));
                return new Halves(fL.get(), fR.get());
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                throw new SQLException("Parallel half-scan interrupted", ie);
            } catch (ExecutionException ee) {
                throw new SQLException("Parallel half-scan failed", ee.getCause());
            } finally {
                es.shutdown();
            }
        }
    }

    /*
     * ═════════════════════════════ ④ Freshness 计算（旧：单次；新：分半并行）
     * ═════════════════════════════
     */

    /** 旧：单次 AP 计算入口（保留） */
    public static long[] computeFreshnessDual(
            Connection apConn,
            Connection tpConn,
            Timestamp lTs,
            Timestamp rTs,
            long queryStartMs) throws Exception {
        ExecutorService ioPool = Executors.newFixedThreadPool(1);
        Future<Long2ObjectOpenHashMap<VisibleRow>> apF = ioPool.submit(
                (Callable<Long2ObjectOpenHashMap<VisibleRow>>) () -> fetchColumnVersions(apConn, lTs, rTs));
        Long2ObjectOpenHashMap<VisibleRow> apRows = apF.get(); // 等 AP
        ioPool.shutdown();

        long t0 = System.currentTimeMillis();
        CompletableFuture<Long> apFreshF = CompletableFuture.supplyAsync(
                () -> calcFreshnessAP(apRows, lTs, rTs, queryStartMs),
                FJP);
        long apFresh = apFreshF.join();
        long durMs = System.currentTimeMillis() - t0;
        long tpFresh = 0;
        System.out.printf(">>> Freshness  AP = %d ms | TP = %d ms, time bound is [%s, %s]%n",
                apFresh, tpFresh, lTs, rTs);
        return new long[] { apFresh, tpFresh, durMs };
    }

    /** 新：并行左右半区分别计算；行级GC可先做；窗口GC等两边完成后统一触发 */
    public static long[] computeFreshnessDualSplitParallel(
            Connection apLeft,
            Connection apRight,
            Connection tpConn, // 目前未用
            Timestamp lTs,
            Timestamp rTs,
            long queryStartMs) throws Exception {

        final long lMs = lTs.getTime(), rMs = rTs.getTime();
        final long midMs = lMs + ((rMs - lMs) >>> 1);
        final Timestamp mid = new Timestamp(midMs);

        // 建议外部保证共享快照（同一事务隔离级别；必要时 setAutoCommit(false)）
        Halves halves = fetchColumnVersionsSplit(apLeft, apRight, lTs, rTs);

        long t0 = System.currentTimeMillis();

        // 并行计算（不做窗口GC）
        CompletableFuture<PartResult> fLeft = CompletableFuture.supplyAsync(
                () -> calcFreshnessAPCore(halves.left, lTs, mid, queryStartMs), FJP);
        CompletableFuture<PartResult> fRight = CompletableFuture.supplyAsync(
                () -> calcFreshnessAPCore(halves.right, mid, rTs, queryStartMs), FJP);

        PartResult leftRes = fLeft.join();
        // 左半行级GC（安全，释放旧版本）
        triggerRowGCBatch(halves.left);
        halves.left.clear();

        PartResult rightRes = fRight.join();
        // 右半行级GC
        triggerRowGCBatch(halves.right);
        halves.right.clear();

        // 窗口GC（必须在两边都完成后统一触发）
        scheduleWindowGC(mid, new AtomicLong(leftRes.maxVisibleCommit));
        scheduleWindowGC(rTs, new AtomicLong(rightRes.maxVisibleCommit));

        long apFresh = Math.max(leftRes.fresh, rightRes.fresh);
        long tpFresh = 0L; // 如需 TP 分支，可后续补齐
        long durMs = System.currentTimeMillis() - t0;

        System.out.printf(">>> AP(left)=%d ms | AP(right)=%d ms | AP(max)=%d ms, window=[%s, %s]%n",
                leftRes.fresh, rightRes.fresh, apFresh, lTs, rTs);

        return new long[] { apFresh, tpFresh, durMs };
    }

    /** 兼容旧签名：默认 cutoff = lTs（行为不变） */
    public static long calcFreshnessAP(Long2ObjectOpenHashMap<VisibleRow> apMap,
            Timestamp lTs, Timestamp now, long qStart) {
        return calcFreshnessAP(apMap, lTs, now, qStart, lTs);
    }

    /** 旧：单次 AP 计算（带 GC），保留。可独立使用。 */
    public static long calcFreshnessAP(Long2ObjectOpenHashMap<VisibleRow> apMap,
            Timestamp lTs,
            Timestamp now,
            long qStart,
            Timestamp gcCutoff) {

        final long lMs = lTs.getTime(), nMs = now.getTime();
        LongAccumulator max = new LongAccumulator(Long::max, 0);
        AtomicLong maxVisibleCommit = new AtomicLong(Long.MIN_VALUE);

        long t = System.nanoTime();
        FJP.submit(() -> TRACE.entrySet().stream().parallel().forEach(ent -> {
            long pk = ent.getKey();
            Row row = ent.getValue();

            long rowMinTs = Long.MAX_VALUE, rowMaxTs = Long.MIN_VALUE;
            for (NavigableMap<Integer, TimePair> vm : row.cols) {
                if (vm == null || vm.isEmpty())
                    continue;
                rowMinTs = Math.min(rowMinTs, vm.firstEntry().getValue().dbTsMs);
                rowMaxTs = Math.max(rowMaxTs, vm.lastEntry().getValue().dbTsMs);
            }
            if (rowMaxTs < lMs || rowMinTs > nMs)
                return;

            VisibleRow vr = apMap.get(pk);
            if (vr != null) { // AP看得见
                long local = 0;
                for (NavigableMap<Integer, TimePair> vm : row.cols) {
                    if (vm == null)
                        continue;
                    TimePair vis = vm.get(vr.ver);
                    if (vis == null)
                        continue;
                    long diff = vm.lastEntry().getValue().commitMs - vis.commitMs;
                    maxVisibleCommit.accumulateAndGet(vis.commitMs, Math::max);
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
                for (TimePair tp : vm.values()) {
                    if (tp.dbTsMs < lMs)
                        continue; // 还没到窗口起点
                    if (tp.dbTsMs > nMs)
                        break; // 已经出了窗口
                    earliestCommit = Math.min(earliestCommit, tp.commitMs);
                }
            }
            if (earliestCommit != Long.MAX_VALUE)
                max.accumulate(qStart - earliestCommit);
        })).join();
        logCost("Step-2 scan TRACE", t);

        // 行级 / 窗口 GC
        t = System.nanoTime();
        triggerRowGCBatch(apMap);
        scheduleWindowGC(gcCutoff, maxVisibleCommit);
        logCost("Step-3 schedule GC", t);

        long result = max.get();
        System.out.printf(">>> maxFresh = %d ms%n", result);
        return result;
    }

    /* ───────────── 新：并行阶段的“无窗口GC”计算核心 ───────────── */
    /** 并行阶段用：只算 freshness，不触发窗口GC；返回 (fresh, maxVisibleCommit) */
    private static final class PartResult {
        final long fresh;
        final long maxVisibleCommit;

        PartResult(long f, long mvc) {
            this.fresh = f;
            this.maxVisibleCommit = mvc;
        }
    }

    private static PartResult calcFreshnessAPCore(Long2ObjectOpenHashMap<VisibleRow> apMap,
            Timestamp lTs,
            Timestamp now,
            long qStart) {
        final long lMs = lTs.getTime(), nMs = now.getTime();
        LongAccumulator max = new LongAccumulator(Long::max, 0);
        AtomicLong maxVisibleCommit = new AtomicLong(Long.MIN_VALUE);

        long t = System.nanoTime();
        FJP.submit(() -> TRACE.entrySet().stream().parallel().forEach(ent -> {
            long pk = ent.getKey();
            Row row = ent.getValue();

            long rowMinTs = Long.MAX_VALUE, rowMaxTs = Long.MIN_VALUE;
            for (NavigableMap<Integer, TimePair> vm : row.cols) {
                if (vm == null || vm.isEmpty())
                    continue;
                rowMinTs = Math.min(rowMinTs, vm.firstEntry().getValue().dbTsMs);
                rowMaxTs = Math.max(rowMaxTs, vm.lastEntry().getValue().dbTsMs);
            }
            if (rowMaxTs < lMs || rowMinTs > nMs)
                return;

            VisibleRow vr = apMap.get(pk);
            if (vr != null) {
                long local = 0;
                for (NavigableMap<Integer, TimePair> vm : row.cols) {
                    if (vm == null)
                        continue;
                    TimePair vis = vm.get(vr.ver);
                    if (vis == null)
                        continue;
                    long diff = vm.lastEntry().getValue().commitMs - vis.commitMs;
                    maxVisibleCommit.accumulateAndGet(vis.commitMs, Math::max);
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
                for (TimePair tp : vm.values()) {
                    if (tp.dbTsMs < lMs)
                        continue;
                    if (tp.dbTsMs > nMs)
                        break;
                    earliestCommit = Math.min(earliestCommit, tp.commitMs);
                }
            }
            if (earliestCommit != Long.MAX_VALUE)
                max.accumulate(qStart - earliestCommit);
        })).join();
        logCost("Step-2 scan TRACE (no-window-GC)", t);

        return new PartResult(max.get(), maxVisibleCommit.get());
    }

    /* ═════════════════════════════ ⑤ GC 相关 ═════════════════════════════ */

    /** 行级 GC：按 AP 可见版本清理历史 */
    private static void triggerRowGCBatch(Long2ObjectOpenHashMap<VisibleRow> apMap) {
        Long2ObjectOpenHashMap<VisibleRow> snapshot = apMap; // 已是局部 map
        GC_EXEC.submit(() -> snapshot.long2ObjectEntrySet()
                .parallelStream()
                .forEach(e -> gcRowUntilVer(e.getLongKey(), e.getValue().ver)));
    }

    private static void gcRowUntilVer(long pk, int visibleVer) {
        Row row = TRACE.get(pk);
        if (row == null)
            return;
        boolean stillHasData = false;
        for (int i = 0; i < 2; i++) {
            NavigableMap<Integer, TimePair> vm = row.cols[i];
            if (vm == null)
                continue;
            vm.headMap(visibleVer, false).clear(); // 只删旧版本
            if (!vm.isEmpty())
                stillHasData = true;
            else
                row.cols[i] = null;
        }
        if (!stillHasData)
            TRACE.remove(pk);
    }

    /** 窗口 GC：删除 dbCurrentTs < cutoff 的所有历史版本（全局） */
    private static void scheduleWindowGC(Timestamp cutoff, AtomicLong maxVisibleCommit) {
        final long cutoffMs = cutoff.getTime();
        long waterMark = maxVisibleCommit.get(); // 当前未启用，可扩展为双阈值
        GC_EXEC.submit(() -> TRACE.entrySet().parallelStream().forEach(ent -> {
            Row row = ent.getValue();
            for (int i = 0; i < 2; i++) {
                NavigableMap<Integer, TimePair> vm = row.cols[i];
                if (vm == null)
                    continue;
                vm.entrySet().removeIf(e -> e.getValue().dbTsMs < cutoffMs);
                // 若需要更激进，可追加：|| e.getValue().commitMs < waterMark
                if (vm.isEmpty())
                    row.cols[i] = null;
            }
            if (row.cols[0] == null && row.cols[1] == null)
                TRACE.remove(ent.getKey());
        }));
    }

    /* ════════════════════════ ⑥ 调试 / 监控 ════════════════════════ */

    /** 仅用于监控 */
    public static Map<Long, Row> getTraceMap() {
        return TRACE;
    }

    /* ════════════════════════ ⑦ TP 分支（保留，可选启用） ════════════════════════ */

    /** TP 行包装 */
    private static final class TpRow {
        final int latestVer;
        final long tpTsMs;

        TpRow(int v, long t) {
            latestVer = v;
            tpTsMs = t;
        }
    }

    /** 一次批量拿 TP 端同窗口内所有 (pk → latestVersion, current_ts) */
    private static Map<Long, TpRow> fetchTpRows(
            Connection tpConn,
            Timestamp lTs,
            Timestamp rTs) throws SQLException {

        String sql = """
                SELECT ol_w_id,ol_d_id,ol_o_id, ol_delivery_d, ol_receipdate, access_version AS ver, current_ts as ts
                FROM   vodka_order_line
                WHERE  current_ts between ? AND ?
                """;
        Map<Long, TpRow> map = new HashMap<>(400000);
        int rowCount = 0;
        try (PreparedStatement ps = tpConn.prepareStatement(sql)) {
            ps.setTimestamp(1, lTs);
            ps.setTimestamp(2, rTs);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    long pk = packPk(rs.getInt(1), rs.getInt(2), rs.getInt(3));
                    int ver = rs.getInt("ver");
                    long ts = rs.getTimestamp("ts").getTime();
                    map.put(pk, new TpRow(ver, ts));
                    rowCount++;
                }
            }
        }
        System.out.println("TP returned " + rowCount);
        return map;
    }

    /**
     * 计算 TP-fresh：
     * • pk ∈ AP ∩ TP : diff = tpTsMs − apTsMs
     * • pk ∈ TP \ AP : diff = queryStart − tpTsMs
     * • pk ∈ AP \ TP : 不计入（只影响 AP-fresh）
     */
    private static long calcFreshnessTP(Long2ObjectOpenHashMap<VisibleRow> apRows,
            Map<Long, TpRow> tpRows,
            long queryStartMs) {

        LongAccumulator max = new LongAccumulator(Long::max, 0);

        tpRows.forEach((pkObj, tp) -> {
            long pk = pkObj; // boxed → primitive
            VisibleRow vr = apRows.get(pk); // O(1)

            long diff = (vr != null)
                    ? tp.tpTsMs - vr.apTsMs // AP 也见到了
                    : queryStartMs - tp.tpTsMs; // 只有 TP 见到
            if (diff > 0)
                max.accumulate(diff);

            // 一致性检查（可选）
            Row row = TRACE.get(pk);
            if (row != null) {
                long latestCommit = Long.MIN_VALUE;
                for (NavigableMap<Integer, TimePair> vm : row.cols) {
                    if (vm != null && !vm.isEmpty())
                        latestCommit = Math.max(latestCommit, vm.lastEntry().getValue().dbTsMs);
                }
                if (latestCommit != Long.MIN_VALUE && latestCommit != tp.tpTsMs) {
                    int[] ids = unpackPk(pk);
                    System.out.printf("[TP-CHK] w=%d d=%d o=%d | tpTs=%s latestCommit=%s%n",
                            ids[0], ids[1], ids[2],
                            new Timestamp(tp.tpTsMs),
                            new Timestamp(latestCommit));
                }
            }
        });

        return max.get();
    }

    /* ─────────────────────────────── 其它（保留占位） ─────────────────────────────── */

    // 仅作占位，不使用
    private static final class ColumnVersion {
        final long pk;
        final byte idx; // 0 或 1
        final int ver;

        ColumnVersion(long pk, byte idx, int ver) {
            this.pk = pk;
            this.idx = idx;
            this.ver = ver;
        }
    }

    // // 如需测试对象内存：
    // public static void main(String[] args) { ... }
}