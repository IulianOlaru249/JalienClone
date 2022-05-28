/**
 *
 */
package utils;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.security.SecureRandom;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;

import alien.monitoring.Timing;
import alien.user.JAKeyStore;
import lazyj.Format;
import lazyj.Utils;

/**
 * @author costing
 * @since Jun 23, 2020
 */
public class ConnectivityTest {
	/**
	 * Test the connectivity to a given host (all IP addresses, by default of "alice-jcentral.cern.ch") and port (default 8097)
	 *
	 * @param args
	 * @throws IOException
	 */
	public static void main(final String[] args) throws IOException {
		final String testTarget = args.length == 0 ? "alice-jcentral.cern.ch" : args[0];

		final InetAddress[] allAddr;

		try (Timing t = new Timing()) {
			System.err.println("Resolving IP addresses of " + testTarget);
			allAddr = InetAddress.getAllByName(testTarget);
			System.err.println("    resolving " + testTarget + " to " + allAddr.length + " took " + Format.point(t.getMillis()) + " ms");
		}

		final int port = args.length < 2 ? 8097 : Integer.parseInt(args[1]);

		final boolean runSSL = args.length < 3 ? true : Utils.stringToBool(args[2], true);

		int ok = 0;
		double okTime = 0;

		int fail = 0;
		double failTime = 0;

		int sslOK = 0;
		double sslHandshakeTime = 0;

		SSLSocketFactory sslSF = null;

		double dnsResolvingTime = 0;

		if (runSSL) {
			try {
				// get factory
				final KeyManagerFactory kmf = KeyManagerFactory.getInstance("SunX509", "SunJSSE");

				System.err.println("Connecting with client cert: " + ((java.security.cert.X509Certificate) JAKeyStore.getKeyStore().getCertificateChain("User.cert")[0]).getSubjectDN());
				// initialize factory, with clientCert(incl. priv+pub)
				kmf.init(JAKeyStore.getKeyStore(), JAKeyStore.pass);

				java.lang.System.setProperty("jdk.tls.client.protocols", "TLSv1.2");
				final SSLContext ssc = SSLContext.getInstance("TLS");

				// initialize SSL with certificate and the trusted CA and pub certs
				ssc.init(kmf.getKeyManagers(), JAKeyStore.trusts, new SecureRandom());

				sslSF = ssc.getSocketFactory();
			}
			catch (final Throwable t) {
				System.err.println("Could not load the client certificate: " + t.getMessage());
				return;
			}
		}

		for (final InetAddress addr : allAddr) {
			final String hostName;

			try (Timing t = new Timing()) {
				hostName = Utils.getHostName(addr.getHostAddress());
				dnsResolvingTime += t.getMillis();
			}

			System.err.print(addr.getHostAddress() + " ( " + hostName + ") / " + port + " : ");

			try (Timing t = new Timing()) {
				try (Socket s = new Socket()) {
					s.connect(new InetSocketAddress(addr, port), 10000);

					okTime += t.getMillis();
					ok++;
					System.err.println("OK, connection established in " + Format.point(t.getMillis()) + " ms");

					if (sslSF != null) {
						try (Timing t2 = new Timing()) {
							s.setSoTimeout(10000);

							try (@SuppressWarnings("resource")
							SSLSocket sslSocket = (SSLSocket) sslSF.createSocket(s, addr.getHostAddress(), port, true)) {
								sslSocket.startHandshake();
								sslOK++;
								sslHandshakeTime += t2.getMillis();

								System.err.println("  SSL handshake time: " + Format.point(t2.getMillis()) + " ms");
							}
							catch (final IOException ioe) {
								System.err.println("  SSL negociation failed after " + Format.point(t2.getMillis()) + " ms with " + ioe.getMessage());
							}
						}
					}
				}
				catch (final IOException ioe) {
					failTime += t.getMillis();
					fail++;
					System.err.println("Fail in " + Format.point(t.getMillis()) + " ms\n    " + ioe.getMessage());
				}
			}
		}

		System.err.println(" ------------ Summary ---------------");
		System.err.print("Connection successful to " + ok + " out of " + (ok + fail) + " addresses");

		if (ok > 0) {
			final double avgConnectionTime = okTime / ok;

			System.err.println(", average TCP connection time was " + Format.point(avgConnectionTime) + " ms");

			if (runSSL) {
				System.err.println("  out of them " + sslOK + " sockets could be upgraded to SSL");

				if (sslOK > 0) {
					final double avgSSLHandshakeTime = sslHandshakeTime / sslOK;

					System.err.println(
							"    average SSL handshake time was " + Format.point(avgSSLHandshakeTime) + " ms (" + Format.point(avgSSLHandshakeTime / avgConnectionTime) + "x avg connection time)");
				}
			}
		}
		else
			System.err.println();

		System.err.print("Connection failed for " + fail + " out of " + (ok + fail) + " addresses");

		if (fail > 0)
			System.err.println(", average time to fail a connection was " + Format.point(failTime / fail) + " ms");
		else
			System.err.println();

		if (allAddr.length > 0)
			System.err.println("DNS reverse lookup average time: " + Format.point(dnsResolvingTime / allAddr.length) + " ms");
	}
}
