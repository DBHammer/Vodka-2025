package benchmark.oltp.schedule;

/**
 * A time interval with a fixed target throughput.
 * Immutable → lock-free sharing.
 */
public final class Segment {
    public final Mode mode;
    public final long startMillis;
    public final long endMillis;
    public final int  tpm;        // txns per minute
    public final long timePerTx;  // millis between txns (=60000/tpm)

    public Segment(Mode mode, long startMillis, long endMillis, int tpm) {
        this.mode        = mode;
        this.startMillis = startMillis;
        this.endMillis   = endMillis;
        this.tpm         = tpm;
        this.timePerTx   = (tpm > 0) ? 60000L / tpm : 0L;
    }

    public boolean contains(long now) {
        return now >= startMillis && now < endMillis;
    }
}