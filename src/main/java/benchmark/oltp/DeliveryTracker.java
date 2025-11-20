package benchmark.oltp;

import java.sql.Timestamp;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@ToString
@Getter
@Setter
public class DeliveryTracker {
    private LinkedHashMap<List<Integer>, Transaction> txMap = new LinkedHashMap<>();
    public static final AtomicInteger executionTimes = new AtomicInteger(1);

    /* ------------ key 工具 ------------ */
    private static List<Integer> pk(int w, int d, int o) {
        return List.of(w, d, o);
    }

    /* ------------ 记录 ------------ */
    public synchronized void logTransaction(int w, int d, int o, Timestamp ts) {
        txMap.put(pk(w, d, o), new Transaction(w, d, o, ts));
    }

    public synchronized void logTransaction(int w, int d, int o, int olNum, Timestamp ts) {
        txMap.put(pk(w, d, o), new Transaction(w, d, o, olNum, ts));
    }

    public synchronized void logTransaction(int w, int d, int o,
            long txid, int olNum, int ver, Timestamp ts) {
        txMap.put(pk(w, d, o), new Transaction(w, d, o, txid, olNum, ver, ts));
    }

    public synchronized void logTransaction(int w, int d, int o,
            long txid, int ver, Timestamp ts) {
        txMap.put(pk(w, d, o), new Transaction(w, d, o, txid, ver, ts));
    }

    public synchronized LinkedHashMap<List<Integer>, Transaction> getTransactionMap() {
        return new LinkedHashMap<List<Integer>, Transaction>(txMap);
    }

    /* ------------ 查询 ------------ */
    public synchronized Transaction getTxn(int w, int d, int o) {
        return txMap.get(pk(w, d, o));
    }

    public synchronized Transaction getLastInsertedTransaction() {
        Transaction last = null;
        for (var e : txMap.values())
            last = e;
        return last;
    }

    /* ------------ 批次快照 ------------ */
    public synchronized LinkedHashMap<List<Integer>, Transaction> snapshotBatch() {
        var snap = txMap;
        txMap = new LinkedHashMap<>();
        return snap;
    }

    /* 调试输出 */
    public void printTransactions() {
        if (txMap.isEmpty()) {
            System.out.println("No transactions to display.");
            return;
        }
        txMap.forEach((k, v) -> System.out.printf("PK=%s, Details=%s%n", k, v));
    }

    /* ------------ Transaction ------------ */
    @Getter
    @Setter
    @ToString
    public static class Transaction {
        public int w_id, ol_d_id, ol_o_id, ol_number, ver;
        public Timestamp current_ts;
        private long tpXid;

        public Transaction(int w, int d, int o, Timestamp ts) {
            this.w_id = w;
            this.ol_d_id = d;
            this.ol_o_id = o;
            this.current_ts = ts;
        }

        public Transaction(int w, int d, int o, int olNum, Timestamp ts) {
            this(w, d, o, ts);
            this.ol_number = olNum;
        }

        public Transaction(int w, int d, int o, long tpXid, int olNum, int ver, Timestamp ts) {
            this(w, d, o, ts);
            this.tpXid = tpXid;
            this.ol_number = olNum;
            this.ver = ver;
        }

        public Transaction(int w, int d, int o, long tpXid, int ver, Timestamp ts) {
            this(w, d, o, ts);
            this.tpXid = tpXid;
            this.ver = ver;
        }
    }
}