package alien.monitoring;

import java.util.Vector;

/**
 * Callback mechanism to notify 3rd parties of newly produced monitoring data
 *
 * @author costing
 * @since 2019-07-03
 */
public interface MonitorDataCallback {

	/**
	 * @param clusterName cluster name
	 * @param nodeName node name
	 * @param paramNames parameter names
	 * @param paramValues parameter values, should have the same number of elements as paramNames, and values on the same indexes
	 */
	public void sendParameters(String clusterName, String nodeName, Vector<String> paramNames, Vector<Object> paramValues);

}
