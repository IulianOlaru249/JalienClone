package alien.test.cassandra;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Vector;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.management.AttributeNotFoundException;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanException;
import javax.management.MBeanServerConnection;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.ReflectionException;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

import alien.config.ConfigUtils;
import alien.monitoring.MonitorFactory;
import apmon.ApMon;
import lazyj.ExtProperties;

/**
 * @author mmmartin
 *
 */
public class CassandraMonitor {

	/**
	 * Full JMX url
	 */
	static String serviceUrl = null;

	/**
	 * Same as above but in a JMXServiceURL object
	 */
	static JMXServiceURL url;

	/**
	 * Connector
	 */
	static JMXConnector jmxc = null;

	/**
	 * MBean
	 */
	static MBeanServerConnection mbsConnection = null;

	private static final Logger logger = ConfigUtils.getLogger(CassandraMonitor.class.getCanonicalName());

	/**
	 * The JVM-wide ApMon sender (jAliEn configured)
	 */
	static final ApMon apmon = MonitorFactory.getApMonSender();

	/**
	 * My host name
	 */
	static String hostName = null;

	/**
	 * On first iteration don't send the deltas, they will only be available from the second iteration on
	 */
	static boolean first = true;

	/**
	 * for passing credentials for password
	 */
	static ExtProperties config = ConfigUtils.getConfiguration("cassandra");

	private static String user = null;
	private static String pass = null;

	private static Map<String, String[]> envpass = new HashMap<>();

	/**
	 * Metrics to export
	 */
	static final String[] metrics = { "org.apache.cassandra.metrics:type=ClientRequest,scope=Write,name=TotalLatency", "org.apache.cassandra.metrics:type=ClientRequest,scope=Read,name=TotalLatency",
			"org.apache.cassandra.metrics:type=ClientRequest,scope=Write,name=Latency", // OMR
			"org.apache.cassandra.metrics:type=ClientRequest,scope=Read,name=Latency", // OMR
			"org.apache.cassandra.metrics:type=ClientRequest,scope=Write,name=Latency", "org.apache.cassandra.metrics:type=ClientRequest,scope=Read,name=Latency",
			"org.apache.cassandra.metrics:type=Cache,scope=KeyCache,name=Hits", // OMR
			"org.apache.cassandra.metrics:type=Cache,scope=KeyCache,name=Requests", // OMR
			"org.apache.cassandra.metrics:type=Storage,name=Load", "org.apache.cassandra.metrics:type=Compaction,name=CompletedTasks", "org.apache.cassandra.metrics:type=Compaction,name=PendingTasks",
			"org.apache.cassandra.metrics:type=Storage,name=Exceptions", "org.apache.cassandra.metrics:type=ClientRequest,scope=Read,name=Timeouts",
			"org.apache.cassandra.metrics:type=ClientRequest,scope=Write,name=Timeouts", "org.apache.cassandra.metrics:type=ClientRequest,scope=Read,name=Unavailables",
			"org.apache.cassandra.metrics:type=ClientRequest,scope=Write,name=Unavailables",
			"org.apache.cassandra.metrics:type=ColumnFamily,keyspace=catalogue,scope=lfn_index,name=TotalDiskSpaceUsed",
			"org.apache.cassandra.metrics:type=ColumnFamily,keyspace=catalogue,scope=lfn_metadata,name=TotalDiskSpaceUsed",
			"org.apache.cassandra.metrics:type=ColumnFamily,keyspace=catalogue,scope=se_lookup,name=TotalDiskSpaceUsed" };

	/**
	 * Attributes to read
	 */
	static final String[] attributes = { "Count", "Count", "OneMinuteRate", "OneMinuteRate", "Count", "Count", "OneMinuteRate", "OneMinuteRate", "Count", "Value", "Value", "Count", "Count", "Count",
			"Count", "Count", "Count", "Count", "Count" };

	/**
	 * Metric names
	 */
	static final String[] names = { "Write_TotalLatency_Count", "Read_TotalLatency_Count", "Write_Latency_OneMinuteRate", "Read_Latency_OneMinuteRate", "Write_Latency_Count", "Read_Latency_Count",
			"KeyCache_Hits_OneMinuteRate", "KeyCache_Requests_OneMinuteRate", "Storage_Load_Count", "Compaction_CompletedTasks_Value", "Compaction_PendingTasks_Value", "Storage_Exceptions_Count",
			"Read_Timeouts_Count", "Write_Timeouts_Count", "Read_Unavailables_Count", "Write_Unavailables_Count", "Disk_Used_lfn_index", "Disk_Used_lfn_metadata", "Disk_Used_se_lookup" };

	/**
	 * Rate flags (counters that are only increasing)
	 */
	static final boolean[] isRate = { false, false, true, true, false, false, true, true, false, false, false, false, false, false, false, false, false, false, false };

	/**
	 * Previous values to compute deltas
	 */
	static double[] previousValues = new double[metrics.length];

	/**
	 * @param args
	 */
	public static void main(final String[] args) {
		if (config == null) {
			logger.severe("cassandra.properties missing?");
			return;
		}
		user = config.gets("cassandraUsername");
		pass = config.gets("cassandraPassword");

		if (user.equals("") || pass.equals("")) {
			logger.severe("cassandra.properties misses some field: cassandraUsername or cassandraPassword");
			return;
		}

		// set serviceUrl
		if (args.length >= 2) {
			final String jmxhost = args[0];
			final String jmxport = args[1];
			serviceUrl = "service:jmx:rmi://" + jmxhost + "/jndi/rmi://" + jmxhost + ":" + jmxport + "/jmxrmi";
		}
		else
			serviceUrl = "service:jmx:rmi://127.0.0.1/jndi/rmi://127.0.0.1:7199/jmxrmi";

		logger.info("Service URL: " + serviceUrl);

		final String[] credentials = { user, pass };
		envpass.put(JMXConnector.CREDENTIALS, credentials);

		try {
			logger.info("Going to create the JmxServiceURL");

			if (args.length >= 3) {
				hostName = args[2];
			}
			else
				hostName = ConfigUtils.getLocalHostname();

			url = new JMXServiceURL(serviceUrl);
		}
		catch (final Exception e1) {
			System.err.println("Exception creating JMXServiceURL or JMXConnector: " + e1);
			logger.log(Level.SEVERE, "Exception creating JMXServiceURL: " + e1);
			System.exit(-1);
		}

		if (metrics.length != attributes.length || metrics.length != names.length || metrics.length != isRate.length) {
			System.err.println("Metrics-attributes-names-isrates don't match");
			logger.log(Level.SEVERE, "Metrics-attributes-names-isrates don't match");
			System.exit(-1);
		}

		logger.info("Start looping...");
		while (true) {
			try {

				final Vector<String> paramNames = new Vector<>();
				final Vector<Object> paramValues = new Vector<>();

				for (int i = 0; i < metrics.length; i++) {
					final Object obj = getMbeanAttributeValue(metrics[i], attributes[i]);
					paramNames.add(names[i]);
					if (obj instanceof Number) {
						final Number value = (Number) obj;
						Double valueToSet = Double.valueOf(value.doubleValue());
						if (!isRate[i]) {
							if (!first)
								valueToSet = Double.valueOf(value.doubleValue() - previousValues[i]);

							previousValues[i] = value.doubleValue();
						}

						insertValue(paramValues, valueToSet);
					}
				}
				try {
					// for (int i = 0; i < previousValues.length; i++)
					// logger.info("PreviousValues " + i + ": " + previousValues[i]);

					if (first) {
						logger.log(Level.INFO, "First pass...waiting for next to calculate deltas");
						first = false;
					}
					else {
						// Calculate latencies, e.g. for read: (ReadTotalLatency1-ReadTotalLatency0)/(ReadLatency1-ReadLatency0)
						// 0 scope=Write,name=TotalLatency, 4 scope=Write,name=Latency
						paramNames.addElement("Write_Latency_Calculated");
						insertValue(paramValues, Double.valueOf(((Double) paramValues.get(0)).doubleValue() / ((Double) paramValues.get(4)).doubleValue()));
						// 1 scope=Read,name=TotalLatency, 5 scope=Read,name=Latency
						paramNames.addElement("Read_Latency_Calculated");
						insertValue(paramValues, Double.valueOf(((Double) paramValues.get(1)).doubleValue() / ((Double) paramValues.get(5)).doubleValue()));

						logger.info("Sending parameters:");
						for (int i = 0; i < paramNames.size(); i++)
							logger.info("  Parameter: " + paramNames.get(i) + " = " + paramValues.get(i));

						apmon.sendParameters("Cassandra_Nodes", hostName, paramNames.size(), paramNames, paramValues);
					}
				}
				catch (final Exception e) {
					logger.log(Level.SEVERE, "Exception sending parameters: " + e);
				}

			}
			catch (AttributeNotFoundException | InstanceNotFoundException | MalformedObjectNameException | MBeanException | ReflectionException | IOException e) {
				logger.log(Level.SEVERE, "Exception getting attribute: " + e);
			}

			logger.log(Level.INFO, "Sleep for 60 seconds...");

			try {
				Thread.sleep(60000);
			}
			catch (final InterruptedException e2) {
				logger.log(Level.SEVERE, "Exception sleeping: " + e2);
			}
		}
	}

	/*
	 * This method avoids errors inserting very small values into ML
	 */
	private static void insertValue(final Vector<Object> paramValues, final Double value) {
		if (value.doubleValue() > Float.MIN_NORMAL)
			paramValues.add(value);
		else
			paramValues.add(Double.valueOf(0));
	}

	/*
	 * Gets the value of a MBean attribute from JMX
	 */
	private static Object getMbeanAttributeValue(final String MbeanObjectName, final String attributeName)
			throws IOException, AttributeNotFoundException, InstanceNotFoundException, MBeanException, ReflectionException, MalformedObjectNameException {
		Object attributeValue = null;
		try {
			jmxc = JMXConnectorFactory.connect(url, envpass);

			mbsConnection = jmxc.getMBeanServerConnection();

			final ObjectName objectName = new ObjectName(MbeanObjectName);
			attributeValue = mbsConnection.getAttribute(objectName, attributeName);

			// try {
			// attributeValue = mbsConnection.getMBeanInfo(objectName);
			// } catch (IntrospectionException e) {
			// e.printStackTrace();
			// }

		}
		finally {
			if (jmxc != null)
				jmxc.close();
		}
		return attributeValue;
	}
}