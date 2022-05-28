package alien.test.cassandra;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.logging.Logger;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.policies.RoundRobinPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;

import alien.config.ConfigUtils;
import lazyj.ExtProperties;

/**
 * @author mmmartin
 *
 */
public class DBCassandra {
	private static DBCassandra dbc = null;
	private static Session session = null;
	private static Cluster cluster = null;
	private static final Logger logger = ConfigUtils.getLogger(DBCassandra.class.getCanonicalName());

	private DBCassandra() {
	}

	private synchronized static void createInstance() {
		if (dbc == null) {
			dbc = new DBCassandra();

			// Create the connection pool
			final PoolingOptions poolingOptions = new PoolingOptions();
			poolingOptions.setConnectionsPerHost(HostDistance.LOCAL, 56, 56).setConnectionsPerHost(HostDistance.REMOTE, 56, 56);

			// SocketOptions socketOptions = new SocketOptions();
			// socketOptions.setReadTimeoutMillis(12000);
			ExtProperties config = ConfigUtils.getConfiguration("cassandra");
			if (config == null) {
				logger.severe("cassandra.properties missing?");
				return;
			}

			String nodes = config.gets("cassandraNodes");
			String user = config.gets("cassandraUsername");
			String pass = config.gets("cassandraPassword");

			if (nodes.equals("") || user.equals("") || pass.equals("")) {
				logger.severe("cassandra.properties misses some field: cassandraNodes or cassandraUsername or cassandraPassword");
				return;
			}

			String[] ns = nodes.split(",");
			String[] addresses = new String[ns.length];
			for (int i = 0; i < ns.length; i++) {
				try {
					addresses[i] = InetAddress.getByName(ns[i]).getHostAddress();
					logger.info("Node address[" + i + "]: " + addresses[i]);
				}
				catch (UnknownHostException e) {
					logger.severe("Cannot create InetAddress from: " + ns[i] + " - Exception: " + e);
				}
			}

			DBCassandra.cluster = Cluster.builder().addContactPoints(addresses).withLoadBalancingPolicy(new TokenAwarePolicy(new RoundRobinPolicy())).withPoolingOptions(poolingOptions)
					// .withSocketOptions(socketOptions)
					.withCredentials(user, pass).build();

			DBCassandra.session = cluster.connect();
		}
	}

	/**
	 * Static 'instance' method
	 *
	 * @return the instance
	 */
	public static Session getInstance() {
		if (dbc == null)
			createInstance();

		return DBCassandra.session;
	}

	/**
	 *
	 */
	public static void shutdown() {
		DBCassandra.session.close();
		DBCassandra.cluster.close();
	}

	@Override
	public Object clone() throws CloneNotSupportedException {
		throw new CloneNotSupportedException();
	}

}
