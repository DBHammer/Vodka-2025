// package benchmark.synchronize.tasks;

// import java.sql.Timestamp;
// import java.util.*;
// import java.util.concurrent.*;
// import java.util.logging.Logger;

// import org.apache.commons.math3.util.Pair;

// import benchmark.synchronize.tasks.ColumnVersion;

// /**
//  * Global HTAP version tracker with <b>automatic</b> row-level + time-window GC.
//  *
//  * Row-level GC : inside {@link #addTransaction}.
//  * Time-window GC: via {@link #scheduleWindowGc(String, Timestamp)}.
//  *
//  * Snapshot for AP query: {@link #snapshot(String)} (read-only, zero GC).
//  */
// public final class TransactionTraceMapNew {

//     /* ───────── helper ───────── */
//     private static final class Row {
//         /** column → (version → commitTime) ; newest version kept at tail. */
//         final ConcurrentMap<String, NavigableMap<Integer, Timestamp>> colMap = new ConcurrentHashMap<>();
//     }

//     /* ───────── per-table mutable trace ───────── */
//     private static final class TableTrace {
//         /** composite-pk → Row (mutable) */
//         final ConcurrentMap<List<Integer>, Row> pkMap = new ConcurrentHashMap<>();

//         void add(List<Integer> pk, String col, int ver, Timestamp ts) {
//             Row row = pkMap.computeIfAbsent(pk, k -> new Row());
//             row.colMap.computeIfAbsent(col, k -> new TreeMap<>())
//                     .put(ver, ts);
//         }

//         /* 真正的行级 GC：删除 < visibleVer 的历史；若行已全部可见则整行移除 */
//         void gcRow(List<Integer> pk, int visibleVer) {
//             Row row = pkMap.get(pk);
//             if (row == null)
//                 return;
//             row.colMap.values().forEach(tree -> tree.headMap(visibleVer, false).clear());
//             row.colMap.entrySet().removeIf(e -> e.getValue().isEmpty());
//             if (row.colMap.isEmpty())
//                 pkMap.remove(pk);
//         }

//         void gcBefore(Timestamp cutoff) { // time-window GC
//             pkMap.forEach((pk, row) -> {
//                 row.colMap.values().forEach(tree -> {
//                     Iterator<Timestamp> it = tree.values().iterator();
//                     while (it.hasNext() && it.next().before(cutoff))
//                         it.remove();
//                 });
//                 row.colMap.entrySet().removeIf(e -> e.getValue().isEmpty());
//                 if (row.colMap.isEmpty())
//                     pkMap.remove(pk);
//             });
//         }

//         /* ------ read API for snapshot ------ */
//         Timestamp commitTime(List<Integer> pk, String col, int ver) {
//             Row row = pkMap.get(pk);
//             if (row == null)
//                 return null;
//             NavigableMap<Integer, Timestamp> tree = row.colMap.get(col);
//             return (tree != null) ? tree.get(ver) : null;
//         }

//         Timestamp latestTime(List<Integer> pk, String col) {
//             Row row = pkMap.get(pk);
//             if (row == null)
//                 return null;
//             NavigableMap<Integer, Timestamp> tree = row.colMap.get(col);
//             return (tree == null || tree.isEmpty()) ? null : tree.lastEntry().getValue();
//         }
//     }

//     public TransactionTraceSnapshot snapshotDeep(String table) {
//         TableTrace src = tables.get(table);
//         if (src == null)
//             return null;

//         // 新建一个空的 TableTrace
//         TableTrace copy = new TableTrace();

//         // 遍历原始 pkMap
//         src.pkMap.forEach((pk, row) -> {
//             // 拷贝主键列表
//             List<Integer> pkCopy = new ArrayList<>(pk);

//             // 深拷贝 Row
//             Row rowCopy = new Row();
//             row.colMap.forEach((column, versionMap) -> {
//                 // 拷贝每个版本映射
//                 NavigableMap<Integer, Timestamp> mapCopy = new TreeMap<>();
//                 versionMap.forEach((ver, ts) -> mapCopy.put(ver, ts));
//                 rowCopy.colMap.put(column, mapCopy);
//             });

//             // 放入新的 pkMap
//             copy.pkMap.put(pkCopy, rowCopy);
//         });

//         return new TransactionTraceSnapshot(copy);
//     }

//     /* ───────── read-only snapshot ───────── */
//     public static final class TransactionTraceSnapshot {
//         private final TableTrace readonly;

//         private TransactionTraceSnapshot(TableTrace source) {
//             readonly = new TableTrace();
//             readonly.pkMap.putAll(new HashMap<>(source.pkMap));
//         }

//         /**
//          * Compute the maximum freshness gap (in ms) and the latest commit timestamp
//          * among a list of ColumnVersion entries. Missing rows or timestamps are
//          * skipped.
//          *
//          * @param cvList list of ColumnVersion objects to inspect
//          * @return a Pair where the first element is the maximum gap in milliseconds,
//          *         and the second is the most recent commit Timestamp among those gaps
//          */
//         public Pair<Long, Timestamp> maxGap(List<ColumnVersion> cvList) {
//             long maxGapMs = 0L;
//             Timestamp globalMaxTs = null;
//             for (ColumnVersion cv : cvList) {
//                 // 1) Lookup the row once
//                 Row row = readonly.pkMap.get(cv.pk);
//                 if (row == null) {
//                     System.out.println("row is null");
//                     continue;
//                 }
//                 // 2) Get the version map for this column
//                 NavigableMap<Integer, Timestamp> versions = row.colMap.get(cv.column);
//                 if (versions == null) {
//                     System.out.println("AP version is null");
//                     continue;
//                 }
//                 // 3) Skip if this is already the latest version
//                 Integer latestVersion = versions.lastEntry().getKey();
//                 if (cv.version == latestVersion) {
//                     System.out.println("Two version is equal");
//                     continue;
//                 }
//                 System.out.println("version inconsistent");
//                 // 4) Fetch commit-time and latest-time
//                 Timestamp verTs = readonly.commitTime(cv.pk, cv.column, cv.version);
//                 Timestamp latTs = readonly.latestTime(cv.pk, cv.column);
//                 System.out.println(cv.version + ", " + verTs + "; " + latestVersion + ", " + latTs);
//                 // 5) Skip any null timestamps
//                 if (verTs == null || latTs == null) {
//                     continue;
//                 }
//                 // 6) Compute the gap and update maxGapMs
//                 long gap = latTs.getTime() - verTs.getTime();
//                 if (gap > maxGapMs) {
//                     maxGapMs = gap;
//                 }
//                 // 7) Update globalMaxTs if this verTs is newer
//                 if (globalMaxTs == null || verTs.after(globalMaxTs)) {
//                     globalMaxTs = verTs;
//                 }
//             }

//             return new Pair<>(maxGapMs, globalMaxTs);
//         }
//     }

//     /* ───────── outer tracker ───────── */
//     private static final Logger LOG = Logger.getLogger(TransactionTraceMapNew.class.getName());

//     private final ConcurrentMap<String, TableTrace> tables = new ConcurrentHashMap<>();
//     private final ExecutorService gcPool = Executors.newCachedThreadPool();

//     /* ---- ingest ------------------------------------------------------------- */
//     public void addTransaction(String table,
//             List<Integer> pk,
//             String column,
//             int version,
//             Timestamp commitTime) {
//         tables.computeIfAbsent(table, t -> new TableTrace())
//                 .add(pk, column, version, commitTime);
//     }

//     /* ---- snapshot for AP query --------------------------------------------- */
//     public TransactionTraceSnapshot snapshot(String table) {
//         TableTrace src = tables.get(table);
//         if (src == null)
//             return null;
//         return new TransactionTraceSnapshot(src);
//     }

//     /* ---- schedule asynchronous time-window GC ------------------------------ */
//     public void scheduleWindowGc(String table, Timestamp cutoff) {
//         TableTrace tt = tables.get(table);
//         if (tt != null && cutoff != null)
//             gcPool.submit(() -> tt.gcBefore(cutoff));
//     }

//     public void scheduleRowGc(String table, List<Integer> pk, int visibleVer) {
//         TableTrace tt = tables.get(table);
//         if (tt != null)
//             gcPool.submit(() -> tt.gcRow(pk, visibleVer));
//     }

//     /* ---- shutdown ---------------------------------------------------------- */
//     public void shutdown() {
//         gcPool.shutdown();
//         try {
//             if (!gcPool.awaitTermination(60, TimeUnit.SECONDS))
//                 gcPool.shutdownNow();
//         } catch (InterruptedException e) {
//             Thread.currentThread().interrupt();
//             gcPool.shutdownNow();
//         }
//     }
// }

package benchmark.synchronize.tasks;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.*;

import java.util.logging.Logger;

import lombok.Getter;

import java.util.logging.Level;

class VersionRange {
    int startVersion;
    int endVersion;
    Timestamp commitTime;

    public VersionRange(int startVersion, int endVersion, Timestamp commitTime) {
        this.startVersion = startVersion;
        this.endVersion = endVersion;
        this.commitTime = commitTime;
    }
}

class ColumnVersion {
    List<Integer> primaryKeys; // 支持复合主键
    String columnName;
    int version;

    public ColumnVersion(List<Integer> primaryKeys, String columnName, int version) {
        this.primaryKeys = primaryKeys;
        this.columnName = columnName;
        this.version = version;
    }
}

class ColumnTrace {
    Map<String, List<VersionRange>> columnVersions; // Map column names to their version ranges

    public ColumnTrace() {
        this.columnVersions = new HashMap<>();
    }

    public void addVersionRange(String columnName, int startVersion, int endVersion, Timestamp commitTime) {
        this.columnVersions.computeIfAbsent(columnName, k -> new ArrayList<>())
                .add(new VersionRange(startVersion, endVersion, commitTime));
    }

    /**
     * 返回此列迹中所有列的版本中的最大提交时间。
     * 
     * @return 最大的提交时间，如果没有版本则返回 null。
     */
    public Timestamp getMaxCommitTimeForAllColumns() {
        return this.columnVersions.values().stream()
                .flatMap(List::stream) // 将所有列的版本范围列表扁平化为一个流
                .map(range -> range.commitTime)
                .max(Timestamp::compareTo)
                .orElse(null); // 使用 Stream API 查找最大时间
    }
}

class TrxTrace {
    Map<List<Integer>, ColumnTrace> primaryKeyToColumnsMap;

    public TrxTrace() {
        this.primaryKeyToColumnsMap = new ConcurrentHashMap<>();
    }

    public void updateColumn(List<Integer> primaryKeys, String columnName, int currentVersion, Timestamp commitTime) {
        ColumnTrace columnTrace = primaryKeyToColumnsMap.computeIfAbsent(primaryKeys, k -> new ColumnTrace());
        if (columnName.equalsIgnoreCase("ol_delivery_d")) {
            columnTrace.addVersionRange("ol_delivery_d", 2, 2, commitTime);
            columnTrace.addVersionRange("ol_receipdate", 2, 2, commitTime);
        } else if (columnName.equalsIgnoreCase("ol_receipdate")) {
            columnTrace.addVersionRange("ol_delivery_d", 3, 3, commitTime);
            columnTrace.addVersionRange("ol_receipdate", 3, 3, commitTime);
        } else {
            System.out.println("fail");
        }
    }

    public void printTrxTrace() {
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        primaryKeyToColumnsMap.forEach((primaryKeys, columnTrace) -> {
            System.out.println("Primary Keys: " + primaryKeys);
            columnTrace.columnVersions.forEach((columnName, versionRanges) -> {
                versionRanges.forEach(range -> {
                    System.out.println("Column: " + columnName + ", Start Version: " + range.startVersion +
                            ", End Version: " + range.endVersion + ", Commit Time: "
                            + dateFormat.format(range.commitTime));
                });
            });
            System.out.println();
        });
    }
}

@Getter
public class TransactionTraceMap {
    private static final Logger LOGGER = Logger.getLogger(TransactionTraceMap.class.getName());
    Map<String, TrxTrace> tableTraces;
    ExecutorService executorService;

    public TransactionTraceMap() {
        tableTraces = new ConcurrentHashMap<>();
        executorService = Executors.newCachedThreadPool();
        createTrace();
    }

    public void createTrace() {
        String tableName = "vodka_order_line";
        tableTraces.put(tableName, new TrxTrace());
    }

    public void addTransaction(List<Integer> primaryKeys, String columnName, int currentVersion, Timestamp commitTime) {
        String tableName = "vodka_order_line";
        TrxTrace trxTrace = tableTraces.get(tableName);
        trxTrace.updateColumn(primaryKeys, columnName, currentVersion, commitTime);
    }

    public long findMaxTimeDifference(String tableName, List<ColumnVersion> columnVersionList, Timestamp lTimestamp) {
        long maxDiff = 0;
        Map<List<Integer>, List<ColumnVersion>> primaryKeyToColumnsMap = new HashMap<>();

        for (ColumnVersion cv : columnVersionList) {
            primaryKeyToColumnsMap.computeIfAbsent(cv.primaryKeys, k -> new ArrayList<>()).add(cv);
        }

        TrxTrace trxTrace = tableTraces.get(tableName);
        if (trxTrace != null) {
            for (Map.Entry<List<Integer>, List<ColumnVersion>> entry : primaryKeyToColumnsMap.entrySet()) {
                List<Integer> primaryKeys = entry.getKey();
                List<ColumnVersion> columns = entry.getValue();

                ColumnTrace columnTrace = trxTrace.primaryKeyToColumnsMap.get(primaryKeys);
                if (columnTrace != null) {
                    boolean allEqual = true;
                    for (ColumnVersion cv : columns) {
                        Timestamp commitTime = getCommitTime(columnTrace, cv.columnName, cv.version);
                        Timestamp latestCommitTime = getLatestCommitTime(columnTrace, cv.columnName);
                        if (commitTime == null || latestCommitTime == null || !commitTime.equals(latestCommitTime)) {
                            allEqual = false;
                            break;
                        } else {
                            long diff = latestCommitTime.getTime() - commitTime.getTime();
                            maxDiff = Math.max(maxDiff, diff);
                        }
                    }

                    // 只有当所有列的时间都相等时才移除
                    if (allEqual) {
                        trxTrace.primaryKeyToColumnsMap.remove(primaryKeys);
                    }
                }
            }
            scheduleCleanupTask(tableName, lTimestamp);
        } else {
            LOGGER.log(Level.WARNING, "No TrxTrace found for table: {0}", tableName);
        }
        LOGGER.log(Level.INFO, "Maximum time difference found: {0}ms", maxDiff);
        return maxDiff;
    }

    private void cleanupOldEntries(String tableName, Timestamp cutoffTime) {
        TrxTrace trxTrace = tableTraces.get(tableName);
        if (trxTrace != null) {
            Iterator<Map.Entry<List<Integer>, ColumnTrace>> it = trxTrace.primaryKeyToColumnsMap.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry<List<Integer>, ColumnTrace> entry = it.next();
                ColumnTrace columnTrace = entry.getValue();
                Timestamp maxCommitTime = columnTrace.getMaxCommitTimeForAllColumns();
                if (maxCommitTime != null && maxCommitTime.before(cutoffTime)) {
                    it.remove();
                }
            }
        }
    }

    private void scheduleCleanupTask(String tableName, Timestamp cutoffTime) {
        executorService.submit(() -> cleanupOldEntries(tableName, cutoffTime));
    }

    private Timestamp getCommitTime(ColumnTrace columnTrace, String columnName, int version) {
        List<VersionRange> ranges = columnTrace.columnVersions.get(columnName);
        if (ranges != null) {
            for (VersionRange range : ranges) {
                if (version >= range.startVersion && version <= range.endVersion) {
                    return range.commitTime;
                }
            }
        }
        return null;
    }

    private Timestamp getLatestCommitTime(ColumnTrace columnTrace, String columnName) {
        List<VersionRange> ranges = columnTrace.columnVersions.get(columnName);
        if (ranges != null && !ranges.isEmpty()) {
            return ranges.get(ranges.size() - 1).commitTime;
        }
        System.out.println("Fail to ger latest commit time");
        return null;
    }

    private int getLatestVersion(ColumnTrace columnTrace, String columnName) {
        List<VersionRange> ranges = columnTrace.columnVersions.get(columnName);
        if (ranges != null && !ranges.isEmpty()) {
            // System.out.println(ranges.get(ranges.size() - 1).endVersion);
            return ranges.get(ranges.size() - 1).endVersion;
        }
        System.out.println("Fail to ger version");
        return 0;
    }

    public void shutdown() {
        executorService.shutdown();
        try {
            if (!executorService.awaitTermination(60, TimeUnit.SECONDS)) {
                executorService.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            executorService.shutdownNow();
        }
    }
}