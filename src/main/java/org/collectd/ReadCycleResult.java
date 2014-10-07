package org.collectd;

import java.util.concurrent.TimeUnit;

/**
 * Results of a read() cycle.
 */
public class ReadCycleResult {
	long started = 0;
	long ended = 0;
	long duration = 0;
	long interval = 0;
	int poolSize = 0;
	int failed = 0;
	int cancelled = 0;
	int success = 0;
	int total = 0;


	public ReadCycleResult(final int failed, final int cancelled, final int success, final long started, final long ended, final long interval) {
		this.failed = failed;
		this.cancelled = cancelled;
		this.success = success;
		this.started = started;
		this.ended = ended;
		this.total = failed + cancelled + success;
		this.duration = ended - started;
		this.interval = interval;
	}

	public int getTotal() {
		return total;
	}

	public int hashCode() {
		return new Double(((success + failed) / duration) * Math.pow(2, poolSize)).hashCode();
	}

	public long getDuration() {
		return TimeUnit.MILLISECONDS.convert(duration, TimeUnit.NANOSECONDS);
	}

	public String toString() {
		return "[failed: " + failed + ", canceled: " + cancelled + ", success: " + success + "] Took " + TimeUnit.MILLISECONDS.convert(duration, TimeUnit.NANOSECONDS) + "ms in a pool of " + poolSize + " threads.";
	}
}
