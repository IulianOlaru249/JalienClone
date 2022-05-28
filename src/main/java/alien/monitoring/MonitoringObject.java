/**
 *
 */
package alien.monitoring;

import java.util.Vector;

/**
 * @author costing
 * @since Nov 10, 2010
 */
public interface MonitoringObject {

	/**
	 * Fill the two vectors with names and values produced by the instance
	 *
	 * @param paramNames
	 * @param paramValues
	 */
	void fillValues(final Vector<String> paramNames, final Vector<Object> paramValues);

}
