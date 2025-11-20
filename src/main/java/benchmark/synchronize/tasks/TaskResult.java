package benchmark.synchronize.tasks;

public class TaskResult {
    public int taskType;
    public long txnCompleteTime;
    public double gapTime;
    public long startTime;
    public long endTime;
    public int tryNum;
    public boolean pass;
    public boolean isApConnErr;
    public long haTtrickFreshness;
    public long vodkaFreshness;
    public long syncLatency;
    public long executeLatecy;
    public long unifiedLatency;

    public TaskResult(int taskType, long txnCompleteTime, double gapTime, long startTime, long endTime,
            int tryNum, boolean pass, boolean isApConnErr, long haTtrickFreshness, long vodkaFreshness, long syncLatency, long executeLatecy, long unifiedLatency) {
        this.taskType = taskType;
        this.txnCompleteTime = txnCompleteTime;
        this.gapTime = gapTime;
        this.startTime = startTime;
        this.endTime = endTime;
        this.tryNum = tryNum;
        this.pass = pass;
        this.isApConnErr = isApConnErr;
        this.haTtrickFreshness = haTtrickFreshness;
        this.vodkaFreshness = vodkaFreshness;
        this.syncLatency = syncLatency;
        this.executeLatecy = executeLatecy;
        this.unifiedLatency = unifiedLatency;
    }
}
