package alien.site.packman;

import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import alien.config.ConfigUtils;
import alien.site.JobAgent;

/**
 *
 */
public abstract class PackMan {

	private static final Logger logger = ConfigUtils.getLogger(JobAgent.class.getCanonicalName());

	/**
	 * @return ?
	 */
	abstract public boolean getHavePath();

	/**
	 * @return all defined packages
	 */
	public abstract List<String> getListPackages();

	/**
	 * @return list of installed packages
	 */
	public abstract List<String> getListInstalledPackages();

	/**
	 * @param packArray
	 */
	public void printPackages(final List<String> packArray) {
		logger.log(Level.INFO, this.getClass().getCanonicalName() + " printing list of packages ");

		for (final String pack : packArray != null ? packArray : getListPackages())
			System.out.println(pack);

		return;
	}

	/**
	 * @return method name
	 */
	@SuppressWarnings("static-method")
	public String getMethod() {
		return "PackMan";
	}

	/**
	 * @param user
	 * @param packages
	 * @param version
	 * @return ?
	 */
	public abstract Map<String, String> installPackage(String user, String packages, String version);

}
