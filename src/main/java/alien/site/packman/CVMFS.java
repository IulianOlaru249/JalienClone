/**
 *
 */
package alien.site.packman;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import alien.api.Dispatcher;
import alien.api.catalogue.GetAliEnv;
import alien.api.catalogue.SetAliEnv;
import alien.config.ConfigUtils;
import alien.config.Version;
import alien.site.JobAgent;
import lazyj.commands.CommandOutput;
import lazyj.commands.SystemCommand;

/**
 * @author mmmartin
 *
 */
public class CVMFS extends PackMan {

	/**
	 * logger object
	 */
	static final Logger logger = ConfigUtils.getLogger(JobAgent.class.getCanonicalName());

	private boolean havePath = true;

	/**
	 * CVMFS paths
	 */
	private final static String CVMFS_BASE_DIR = "/cvmfs/alice.cern.ch";
	private final static String JAVA32_DIR = CVMFS_BASE_DIR + "/java/JDKs/i686/jdk-latest/bin";
	private static String ALIEN_BIN_DIR = CVMFS_BASE_DIR + "/bin";

	/**
	 * Constructor just checks CVMFS bin exist
	 *
	 * @param location
	 */
	public CVMFS(final String location) {
		if (location != null && !location.isBlank()) {
			if (Files.exists(Paths.get(location + (location.endsWith("/") ? "" : "/") + "alienv")))
				ALIEN_BIN_DIR = location;
			else {
				havePath = false;
				ALIEN_BIN_DIR = null;
			}
		}
	}

	/**
	 * returns if alienv was found on the system
	 */
	@Override
	public boolean getHavePath() {
		return havePath;
	}

	/**
	 * get the list of packages in CVMFS, returns an array
	 */
	@Override
	public List<String> getListPackages() {
		logger.log(Level.INFO, "PackMan-CVMFS: Getting list of packages ");

		if (this.getHavePath()) {
			final String listPackages = SystemCommand.bash(ALIEN_BIN_DIR + "/alienv q --packman").stdout;
			return Arrays.asList(listPackages.split("\n"));
		}

		return null;
	}

	/**
	 * get the list of installed packages in CVMFS, returns an array
	 */
	@Override
	public List<String> getListInstalledPackages() {
		logger.log(Level.INFO, "PackMan-CVMFS: Getting list of packages ");

		if (this.getHavePath()) {
			final String listPackages = SystemCommand.bash(ALIEN_BIN_DIR + "/alienv q --packman").stdout;
			return Arrays.asList(listPackages.split("\n"));
		}
		return null;
	}

	@Override
	public String getMethod() {
		return "CVMFS";
	}

	@Override
	public Map<String, String> installPackage(final String user, final String packages, final String version) {
		final HashMap<String, String> environment = new HashMap<>();
		String args = packages;

		if (version != null)
			args += "/" + version;

		final String source = getAliEnPrintenv(args);

		if (source == null)
			return null;

		final ArrayList<String> parts = new ArrayList<>(Arrays.asList(source.split(";")));
		parts.remove(parts.size() - 1);

		for (final String value : parts)
			if (!value.contains("export")) {
				final String[] str = value.split("=");

				if (str[1].contains("\\"))
					str[1] = str[1].replace("\\", "");

				environment.put(str[0], str[1].trim()); // alienv adds a space at the end of each entry
			}

		return environment;
	}

	private static String getAliEnPrintenv(final String args) {
		final String keyModifier = SystemCommand.bash("lsb_release -s -d").stdout;

		try {
			logger.log(Level.INFO, "Executing GetAliEnv");
			final GetAliEnv env = Dispatcher.execute(new GetAliEnv(args, keyModifier));

			if (env.getCachedAliEnOutput() != null) {
				logger.log(Level.INFO, "We have cached alienv: " + env.getCachedAliEnOutput());
				return env.getCachedAliEnOutput();

			}
		}
		catch (final Exception e) {
			logger.log(Level.WARNING, "Exception executing GetAliEnv", e);
		}

		final CommandOutput co = SystemCommand.bash(ALIEN_BIN_DIR + "/alienv printenv " + args);

		String source = co.stdout;

		if (source.isBlank()) {
			logger.log(Level.SEVERE, "alienv didn't return anything useful");
			return null;
		}

		final String stderr = co.stderr;

		if (stderr.contains("ERROR:")) {
			logger.log(Level.SEVERE, "alienv returned an error: " + stderr);
			return null;
		}
		
		//remove newline between entries, in case of modules v4
		source = source.replace("\n", "").replace("\r", "");

		try {
			logger.log(Level.INFO, "Executing SetAliEnv");
			Dispatcher.execute(new SetAliEnv(args, keyModifier, source));
		}
		catch (final Exception e) {
			logger.log(Level.WARNING, "Exception executing SetAliEnv", e);
		}

		logger.log(Level.INFO, "GetAliEnPrintenv done");
		return source;
	}

	/**
	 * @return the command to get the full environment to run JAliEn components
	 */
	public static String getAlienvPrint() {
		final String jalienEnvPrint = ALIEN_BIN_DIR + "/alienv printenv JAliEn";

		final String versionFromProps = ConfigUtils.getConfiguration("version").gets("jobagent.version");
		if (versionFromProps != null && !versionFromProps.isBlank())
			return jalienEnvPrint + "/" + versionFromProps;

		if (!Version.getTagFromEnv().isEmpty())
			return jalienEnvPrint + Version.getTagFromEnv();

		return jalienEnvPrint + "/" + Version.getTag() + "-1";
	}

	/**
	 * @return 32b JRE location in CVMFS, to be used for all WN activities due to its much lower virtual memory footprint
	 */
	public static String getJava32Dir() {
		return JAVA32_DIR;
	}

	/**
	 * @return location of script used for cleanup of stale processes
	 */
	public static String getCleanupScript() {
		return CVMFS_BASE_DIR + "/scripts/ja_cleanup.pl";
	}

	/**
	 * @return location of script used for LHCbMarks
	 */
	public static String getLhcbMarksScript() {
		return CVMFS_BASE_DIR + "/scripts/lhcbmarks.sh";
	}

	/**
	 * @return path to job container
	 */
	public static String getContainerPath() {
		return CVMFS_BASE_DIR + "/containers/fs/singularity/centos7";
	}

	/**
	 * @return path to Apptainer runtime in CVMFS
	 */
	public static String getApptainerPath() {
		return CVMFS_BASE_DIR + "/containers/bin/apptainer/current/bin";
	}
	
	/**
	 * @return path to Singularity runtime in CVMFS
	 */
	public static String getSingularityPath() {
		return CVMFS_BASE_DIR + "/containers/bin/singularity/current/bin";
	}

	/**
	 * @return the CVMFS revision of the alice.cern.ch repository mounted on the machine
	 */
	public static int getRevision() {
		try {
			return Integer.parseInt(SystemCommand.bash("LD_LIBRARY_PATH= attr -qg revision /cvmfs/alice.cern.ch").stdout.trim());
		}
		catch (@SuppressWarnings("unused") final Exception e) {
			return -1;
		}
	}
}
