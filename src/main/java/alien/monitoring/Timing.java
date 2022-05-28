package alien.monitoring;

import java.io.Closeable;

import lazyj.Format;

/**
 * @author costing
 * @since 2019-06-12
 */
public final class Timing implements Closeable {

	private long startedTimestamp;
	private long endedTimestamp = -1;

	private Monitor monitor = null;
	private String key = null;

	/**
	 * Initialize the timer with the current nano time
	 */
	public Timing() {
		startedTimestamp = System.nanoTime();
	}

	/**
	 * Initialize the timer with the current nano time and a target monitor object
	 *
	 * @param monitor
	 * @param key
	 */
	public Timing(final Monitor monitor, final String key) {
		this();

		this.monitor = monitor;
		this.key = key;
	}

	/**
	 * (Re)start the measurement
	 */
	public void startTiming() {
		startedTimestamp = System.nanoTime();
		endedTimestamp = -1;
	}

	/**
	 * @param startTime new start time, in nanos
	 * @see System#nanoTime()
	 */
	public void setStartTime(final long startTime) {
		startedTimestamp = startTime;
	}

	/**
	 * Stop the measurement
	 */
	public void endTiming() {
		endedTimestamp = System.nanoTime();
	}

	/**
	 * Close the measurement interval, but only if the end time is after the previously set start time
	 *
	 * @param endTime new end time, in nanoseconds
	 * @see System#nanoTime()
	 */
	public void setEndTime(final long endTime) {
		if (endTime >= startedTimestamp)
			endedTimestamp = endTime;
	}

	/**
	 * @return the duration of this measurement (so far or until stopped), in nanoseconds
	 */
	public long getNanos() {
		return (endedTimestamp >= startedTimestamp ? endedTimestamp : System.nanoTime()) - startedTimestamp;
	}

	/**
	 * @return duration of this measurement, either until current time or when it was last stopped, in milliseconds
	 */
	public double getMillis() {
		return getNanos() / 1000000.;
	}

	/**
	 * @return duration of this measurement, either until current time or when it was last stopped, in seconds
	 */
	public double getSeconds() {
		return getNanos() / 1000000000.;
	}

	@Override
	public void close() {
		if (monitor != null)
			monitor.addMeasurement(key, this);
	}

	@Override
	public String toString() {
		return Format.toInterval(Math.round(getMillis()));
	}
}
