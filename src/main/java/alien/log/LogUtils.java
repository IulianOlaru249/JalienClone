package alien.log;

import java.io.IOException;
import java.util.logging.FileHandler;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

/**
 * @author mmmartin
 *
 */
public class LogUtils {

	/**
	 * @param logger
	 * @param location
	 * @return the logger
	 */
	public static Logger redirectToCustomHandler(Logger logger, String location) {
		// We close current handlers
		Handler[] handlers = logger.getHandlers();
		for (Handler handler : handlers) {
			logger.removeHandler(handler);
		}

		// Read props
		LogManager logManager = LogManager.getLogManager();

		// Create new handler
		Handler fh = null;
		try {
			fh = new FileHandler(location + ".log.%g", Integer.parseInt(logManager.getProperty("java.util.logging.FileHandler.limit")),
					Integer.parseInt(logManager.getProperty("java.util.logging.FileHandler.count")), Boolean.parseBoolean(logManager.getProperty("java.util.logging.FileHandler.append")));

			fh.setLevel(Level.INFO);
			fh.setFormatter(new SimpleFormatter());

			logger.addHandler(fh);
			logger.setLevel(Level.FINE);
		}
		catch (SecurityException | IOException e) {
			System.err.println("Cannot configure the logging manager: " + e.getMessage());
		}

		return logger;
	}

}
