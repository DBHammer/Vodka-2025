package benchmark.oltp.schedule;

/** Three throughput modes used by the dynamic scheduler. */
public enum Mode {
    LOW,        // baseline – delta
    MEDIUM,     // baseline
    HIGH        // baseline + delta
}