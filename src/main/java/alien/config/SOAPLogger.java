package alien.config;

import java.util.logging.Level;
import java.util.logging.LogRecord;

/**
 * @author costing
 *
 */
public class SOAPLogger {

	private final StringBuilder loggingBuffer = new StringBuilder();

	private final int level;

	/**
	 * @param level
	 */
	public SOAPLogger(final int level) {
		this.level = level;
	}

	/**
	 * @param record record to log
	 */
	public void log(final LogRecord record) {
		final int l = record.getLevel().intValue();

		if ((l >= Level.WARNING.intValue()) ||
				(l >= Level.INFO.intValue() && level >= 1) ||
				(l >= Level.CONFIG.intValue() && level >= 2) ||
				(l >= Level.FINE.intValue() && level >= 3) ||
				(l >= Level.FINER.intValue() && level >= 4) ||
				(level >= 5)) {

			loggingBuffer.append(record.getLevel().getName()).append(": ").append(record.getMessage()).append('\n');
		}
	}

	@Override
	public String toString() {
		return loggingBuffer.toString();
	}
}
