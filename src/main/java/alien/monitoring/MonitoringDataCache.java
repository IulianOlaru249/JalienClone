package alien.monitoring;

import java.util.AbstractMap;
import java.util.Map;
import java.util.Vector;

import lazyj.cache.ExpirationCache;

/**
 * Keep a cache of the last sent monitoring parameters. By default they expire in 5 minutes.
 *
 * @author costing
 * @since 2019-07-03
 */
public class MonitoringDataCache extends ExpirationCache<String, Map.Entry<Long, Object>> implements MonitorDataCallback {

	/**
	 * 
	 */
	public MonitoringDataCache() {
		super(10240);
	}
	
	private long ttl = 300000;

	@Override
	public void sendParameters(final String clusterName, final String nodeName, final Vector<String> paramNames, final Vector<Object> paramValues) {
		final Long now = Long.valueOf(System.currentTimeMillis());

		for (int i = 0; i < paramNames.size(); i++)
			overwrite(clusterName + "/" + nodeName + "/" + paramNames.get(i), new AbstractMap.SimpleEntry<>(now, paramValues.get(i)), ttl);
	}

	/**
	 * Set the time to live of the monitoring parameters in the cache
	 *
	 * @param newTTL
	 * @return the old ttl value
	 */
	public long setTTL(final long newTTL) {
		final long oldValue = ttl;

		ttl = newTTL;

		return oldValue;
	}
}
