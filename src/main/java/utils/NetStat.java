package utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.HashSet;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import alien.config.ConfigUtils;

/**
 * netstat-like utility class to support server socket classes
 */

/**
 * @author costing
 * @since Jun 12, 2020
 */
public class NetStat {
	/**
	 * Logger
	 */
	static final Logger logger = ConfigUtils.getLogger(NetStat.class.getCanonicalName());

	private static int lastSetSize = 0;

	/**
	 * Get all the clients connected (ESTABLISHED TCP sockets) to the given local server port number
	 * 
	 * @param serverPort
	 *            server port number
	 * @return all remote sockets connected to this local port number, on both IPv4 and IPv6. Returns <code>null</code> if the statistics gathering didn't work for any reason
	 */
	public static Set<InetSocketAddress> getRemoteEndpoints(final int serverPort) {
		final Set<InetSocketAddress> ret = new HashSet<>(lastSetSize > 16 ? lastSetSize * 4 / 3 : 16);

		final String lookFor = String.format(":%04X ", Integer.valueOf(serverPort));

		final boolean ipv4OK = fillTCP(ret, false, lookFor);
		final boolean ipv6OK = fillTCP(ret, true, lookFor);

		lastSetSize = ret.size();

		return ipv4OK || ipv6OK ? ret : null;
	}

	private static boolean fillTCP(final Set<InetSocketAddress> ret, final boolean ipv6, final String lookFor) {
		final File f = new File(ipv6 ? "/proc/net/tcp6" : "/proc/net/tcp");

		if (!f.exists() || !f.canRead())
			return false;

		/*
		 * sl local_address rem_address st tx_queue rx_queue tr tm->when retrnsmt uid timeout inode
		 * 0: 00000000:006F 00000000:0000 0A 00000000:00000000 00:00000000 00000000 0 0 19857 1 0000000000000000 100 0 0 10 0
		 */
		try (BufferedReader br = new BufferedReader(new FileReader(f))) {
			String line;

			// skip header
			br.readLine();

			while ((line = br.readLine()) != null) {
				// quick check if there is any chance the line would match
				if (!line.contains(lookFor))
					continue;

				int idx = line.indexOf(':');
				final int afterAddr = line.indexOf(' ', idx + 6);

				final String localAddress = line.substring(idx + 2, afterAddr + 1);

				// actual match to the local server port number
				if (!localAddress.endsWith(lookFor))
					continue;

				final int afterRemote = line.indexOf(' ', afterAddr + 6);

				final int afterState = line.indexOf(' ', afterRemote + 1);

				final String state = line.substring(afterRemote + 1, afterState);

				// ESTABLISHED connections, as per https://git.kernel.org/pub/scm/linux/kernel/git/torvalds/linux.git/tree/include/net/tcp_states.h
				if (!state.equals("01"))
					continue;

				final String remoteAddress = line.substring(afterAddr + 1, afterRemote);

				idx = remoteAddress.indexOf(':');

				final String addr = remoteAddress.substring(0, idx);
				final String port = remoteAddress.substring(idx + 1);

				final InetSocketAddress toAdd = new InetSocketAddress(ipv6 ? hexToIPv6(addr) : hex2IPv4(addr), Integer.parseInt(port, 16));

				ret.add(toAdd);
			}
		}
		catch (final Exception e) {
			logger.log(Level.SEVERE, "Exception parsing the content of " + f.getAbsolutePath(), e);
			return false;
		}

		return true;
	}

	private static String hexToIPv6(final String hexaIP) {
		final StringBuilder result = new StringBuilder(48);

		for (int i = 0; i < hexaIP.length(); i = i + 8) {
			final String word = hexaIP.substring(i, i + 8);
			for (int j = word.length() - 1; j >= 0; j = j - 2) {
				result.append(word.substring(j - 1, j + 1));

				if (j == 5)
					result.append(':');
			}

			if (i < hexaIP.length() - 8)
				result.append(":");
		}

		return result.toString();
	}

	private static String hex2IPv4(final String hexa) {
		final StringBuilder result = new StringBuilder(16);

		for (int i = hexa.length() - 1; i >= 0; i = i - 2) {
			final String group = hexa.substring(i - 1, i + 1);
			result.append(Integer.parseInt(group, 16));
			if (i > 1)
				result.append(".");
		}

		return result.toString();
	}

	/**
	 * Debug method
	 *
	 * @param args
	 * @throws Exception
	 */
	public static void main(final String[] args) throws Exception {
		final int port = args.length > 0 ? Integer.parseInt(args[0]) : 8097;

		final int iterations = args.length > 1 ? Integer.parseInt(args[1]) : 1000;

		System.err.println("Timing " + iterations + " times how many clients are connected to the server port number " + port);

		final long nanos = System.nanoTime();

		Set<InetSocketAddress> clients = null;

		for (int i = 0; i < iterations; i++)
			clients = getRemoteEndpoints(port);

		final long deltaNano = System.nanoTime() - nanos;

		if (clients != null) {
			for (final SocketAddress addr : clients)
				System.err.println(addr);

			System.err.println(clients.size() + " total addresses are currently connected to port " + port);
		}

		System.err.println("Gathering took " + (deltaNano / 1000000.) + " ms for " + iterations + " iterations, so " + (deltaNano / 1000000. / iterations) + "ms/call");
	}
}
