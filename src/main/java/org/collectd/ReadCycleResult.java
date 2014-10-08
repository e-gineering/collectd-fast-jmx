package org.collectd;

import org.collectd.api.Collectd;

import java.util.concurrent.TimeUnit;

/**
 * Results of a read() cycle.
 */
public class ReadCycleResult {
	private long started = 0;
	private long ended = 0;
	private long duration = 0;
	private long interval = 0;
	private int poolSize = 0;
	private int failed = 0;
	private int cancelled = 0;
	private int success = 0;
	private int total = 0;

	public ReadCycleResult(final int failed, final int cancelled, final int success, final long started, final long ended, final int poolSize, final long interval) {
		this.failed = failed;
		this.cancelled = cancelled;
		this.success = success;
		this.started = started;
		this.ended = ended;
		this.total = failed + cancelled + success;
		this.duration = ended - started;
		this.poolSize = poolSize;
		this.interval = TimeUnit.NANOSECONDS.convert(interval, TimeUnit.MILLISECONDS);
	}

	public int getPoolSize() {
		return poolSize;
	}

	public int getTotal() {
		return total;
	}

	public int hashCode() {
		return new Double(((success + failed) / duration) * Math.pow(2, poolSize)).hashCode();
	}

	public long getStarted() {
		return started;
	}

	/**
	 * Comparing this cycle to the other one, should we recalculate for optimal pool size?
	 * @param previousCycle
	 * @return
	 */
	public boolean triggerRecalculate(ReadCycleResult previousCycle) {
		if (previousCycle != null) {
			if (previousCycle.poolSize != poolSize) {
				Collectd.logDebug("FastJMX Plugin: triggering recalculation due to pool size change");
				return true;
			} else if (this.cancelled > 0 && previousCycle.cancelled > 0 && this.cancelled > previousCycle.cancelled) {
				Collectd.logDebug("FastJMX Plugin: trigger recalculation due to trend of increasing cancellations");
				return true;
			}
		}
		return false;
	}

	public int getCancelled() {
		return cancelled;
	}

	/**
	 * Returns a Double between 0 and 2.0 to serve as the jacobian weight for this ReadCycleResult.
	 * (total - cancellations / total) * ((interval - duration) / interval)
	 */
	public double getWeight() {
		return (((double)total - cancelled) / total) + (((double)interval - duration) / interval);
	}

	public long getDurationMs() {
		return TimeUnit.MILLISECONDS.convert(duration, TimeUnit.NANOSECONDS);
	}

	public String toString() {
		return "[failed: " + failed + ", canceled: " + cancelled + ", success: " + success + "] Took " + TimeUnit.MILLISECONDS.convert(duration, TimeUnit.NANOSECONDS) + "ms in a pool of " + poolSize + " threads.";
	}
}
