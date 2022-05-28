package alien.monitoring;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Vector;
import java.util.logging.Level;
import java.util.logging.Logger;

import alien.config.ConfigUtils;
import apmon.ApMon;
import apmon.ApMonException;

/**
 * Send the collected monitoring data to any number of interested parties, ApMon being the default one
 *
 * @author costing
 * @since 2019-07-03
 */
public class MonitorDataSender extends ArrayList<MonitorDataCallback> {
	/**
	 *
	 */
	private static final long serialVersionUID = -2966619611925767246L;

	/**
	 * Logger
	 */
	static transient final Logger logger = ConfigUtils.getLogger(MonitorDataSender.class.getCanonicalName());

	private transient final ApMon apmon;

	/**
	 * @param apmon one special target, the ApMon sender. More can be {@link #add(MonitorDataCallback)}-ed and the rest of the management through the List interface works as expected.
	 */
	public MonitorDataSender(final ApMon apmon) {
		this.apmon = apmon;

		if (apmon != null)
			add((clustername, nodename, paramNames, paramValues) -> {
				try {
					apmon.sendParameters(clustername, nodename, paramNames.size(), paramNames, paramValues);
				}
				catch (ApMonException | IOException e) {
					logger.log(Level.SEVERE, "Cannot send ApMon datagram", e);
				}
			});
	}

	/**
	 * @return the ApMon instance that was passed to the constructor. Can be <code>null</code>.
	 */
	public ApMon getApMonInstance() {
		return apmon;
	}

	/**
	 * Send the values to all the interested parties
	 *
	 * @param clusterName
	 * @param nodeName
	 * @param paramNames
	 * @param paramValues
	 */
	public synchronized void sendParameters(final String clusterName, final String nodeName, final Vector<String> paramNames, final Vector<Object> paramValues) {
		if (logger.isLoggable(Level.FINEST))
			logger.log(Level.FINEST, "Sending on " + clusterName + " / " + nodeName + "\n" + paramNames + "\n" + paramValues);

		for (final MonitorDataCallback sender : this) {
			sender.sendParameters(clusterName, nodeName, paramNames, paramValues);
		}
	}

	@Override
	public synchronized boolean add(final MonitorDataCallback e) {
		return super.add(e);
	}
}
