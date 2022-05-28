package alien.catalogue;

import java.io.BufferedReader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import alien.config.ConfigUtils;
import alien.monitoring.Monitor;
import alien.monitoring.MonitorFactory;
import alien.taskQueue.JDL;
import lazyj.DBFunctions;
import lia.util.process.ExternalProcess.ExitStatus;
import utils.ProcessWithTimeout;

/**
 * @author ron
 * @since Nov 23, 2011
 */
public class PackageUtils {
	/**
	 * Logger
	 */
	static final Logger logger = ConfigUtils.getLogger(IndexTableEntry.class.getCanonicalName());

	/**
	 * Monitoring component
	 */
	static final Monitor monitor = MonitorFactory.getMonitor(IndexTableEntry.class.getCanonicalName());

	private static long lastCacheCheck = 0;

	private static Map<String, Package> packages = null;

	private static Set<String> cvmfsPackages = null;

	private static final class BackgroundPackageRefresher extends Thread {
		private final Object wakeupObject = new Object();

		public BackgroundPackageRefresher() {
			setName("alien.catalogue.PackageUtils.BackgroundPackageRefresher");
			setDaemon(true);
		}

		@Override
		public void run() {
			while (true) {
				synchronized (wakeupObject) {
					try {
						wakeupObject.wait(1000 * 60 * 60);
					}
					catch (@SuppressWarnings("unused") final InterruptedException e) {
						// ignore
					}
				}

				cacheCheck();
			}
		}

		public void wakeup() {
			synchronized (wakeupObject) {
				wakeupObject.notifyAll();
			}
		}
	}

	private static final BackgroundPackageRefresher BACKGROUND_REFRESHER = new BackgroundPackageRefresher();

	static {
		BACKGROUND_REFRESHER.start();
	}

	private static synchronized void cacheCheck() {
		if ((System.currentTimeMillis() - lastCacheCheck) > 1000 * 60) {
			final Map<String, Package> newPackages = new LinkedHashMap<>();

			try (DBFunctions db = ConfigUtils.getDB("alice_users")) {
				if (db != null) {
					if (monitor != null)
						monitor.incrementCounter("Package_db_lookup");

					final String q = "SELECT DISTINCT packageVersion, packageName, username, platform, lfn FROM PACKAGES ORDER BY 3,2,1,4,5;";

					db.setReadOnly(true);
					db.setQueryTimeout(60);

					if (!db.query(q))
						return;

					Package prev = null;

					while (db.moveNext()) {
						final Package next = new Package(db);

						if (prev != null && next.equals(prev))
							prev.setLFN(db.gets("platform"), db.gets("lfn"));
						else {
							next.setLFN(db.gets("platform"), db.gets("lfn"));
							prev = next;

							newPackages.put(next.getFullName(), next);
						}
					}
				}
			}

			final Set<String> newCvmfsPackages = new HashSet<>();

			try {
				final ProcessBuilder pBuilder = new ProcessBuilder("/cvmfs/alice.cern.ch/bin/alienv", "q");

				pBuilder.environment().put("LD_LIBRARY_PATH", "");
				pBuilder.environment().put("DYLD_LIBRARY_PATH", "");
				pBuilder.redirectErrorStream(false);

				final Process p = pBuilder.start();

				final ProcessWithTimeout pTimeout = new ProcessWithTimeout(p, pBuilder);

				pTimeout.waitFor(60, TimeUnit.SECONDS);

				final ExitStatus exitStatus = pTimeout.getExitStatus();

				if (exitStatus.getExtProcExitStatus() == 0) {
					final BufferedReader br = new BufferedReader(new StringReader(exitStatus.getStdOut()));

					String line;

					while ((line = br.readLine()) != null)
						newCvmfsPackages.add(line.trim());
				}
			}
			catch (final Throwable t) {
				logger.log(Level.WARNING, "Exception getting the CVMFS package list", t);
			}

			lastCacheCheck = System.currentTimeMillis();

			if (newPackages.size() > 0 || packages == null)
				packages = newPackages;

			if (newCvmfsPackages.size() > 0 || cvmfsPackages == null)
				cvmfsPackages = newCvmfsPackages;
		}
	}

	/**
	 * Force a reload of package list from the database.
	 */
	public static void refresh() {
		lastCacheCheck = 0;

		BACKGROUND_REFRESHER.wakeup();
	}

	/**
	 * @return list of defined packages
	 */
	public static List<Package> getPackages() {
		if (packages == null || packages.size() == 0)
			cacheCheck();
		else
			BACKGROUND_REFRESHER.wakeup();

		if (packages != null)
			return new ArrayList<>(packages.values());

		return null;
	}

	/**
	 * @return the set of known package names
	 */
	public static Set<String> getPackageNames() {
		if (packages == null || packages.size() == 0)
			cacheCheck();
		else
			BACKGROUND_REFRESHER.wakeup();

		if (packages != null)
			return packages.keySet();

		return null;
	}

	/**
	 * Get the Package object corresponding to the given textual description.
	 *
	 * @param name
	 *            package name, eg. "VO_ALICE@AliRoot::vAN-20140917"
	 * @return the corresponding Package object, if it exists
	 */
	public static Package getPackage(final String name) {
		if (packages == null || packages.size() == 0)
			cacheCheck();
		else
			BACKGROUND_REFRESHER.wakeup();

		if (packages != null)
			return packages.get(name);

		return null;
	}

	/**
	 * Get the set of packages registered in CVMFS (should be a subset of the AliEn packages)
	 *
	 * @return set of packages
	 */
	public static Set<String> getCvmfsPackages() {
		if (cvmfsPackages == null)
			cacheCheck();
		else
			BACKGROUND_REFRESHER.wakeup();

		return cvmfsPackages;
	}

	/**
	 * @param j
	 *            JDL to check
	 * @return <code>null</code> if the requirements are met and the JDL can be submitted, or a String object with the message detailing what condition was not met.
	 */
	public static String checkPackageRequirements(final JDL j) {
		if (j == null)
			return "JDL is null";

		if (packages == null)
			cacheCheck();
		else
			BACKGROUND_REFRESHER.wakeup();

		if (packages == null)
			return "Package list could not be fetched from the database";

		final Collection<String> packageVersions = j.getList("Packages");

		if (packageVersions == null || packageVersions.size() == 0)
			return null;

		for (final String requiredPackage : packageVersions) {
			if (!packages.containsKey(requiredPackage))
				return "Package not defined: " + requiredPackage;

			if (cvmfsPackages != null && cvmfsPackages.size() > 0 && !cvmfsPackages.contains(requiredPackage))
				return "Package not seen yet in CVMFS: " + requiredPackage;
		}

		return null;
	}
}
