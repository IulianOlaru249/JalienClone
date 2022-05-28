package alien.catalogue;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import alien.api.Dispatcher;
import alien.api.ServerException;
import alien.api.catalogue.LFNListingfromString;
import alien.config.ConfigUtils;
import alien.monitoring.Monitor;
import alien.monitoring.MonitorFactory;
import alien.user.AliEnPrincipal;
import alien.user.AuthorizationChecker;
import alien.user.UsersHelper;
import lazyj.Format;

/**
 * @author ron
 * @since Mai 28, 2011
 * @author sraje (Shikhar Raje, IIIT Hyderabad)
 * @since Modified July 5, 2012
 */
@SuppressWarnings("unused")
public final class FileSystemUtils {

	/**
	 * Logger
	 */
	static final Logger logger = ConfigUtils.getLogger(CatalogueUtils.class.getCanonicalName());

	/**
	 * Monitoring component
	 */
	static final Monitor monitor = MonitorFactory.getMonitor(CatalogueUtils.class.getCanonicalName());

	/**
	 * @param user
	 * @param directory
	 * @return the LFN
	 */
	public static LFN createCatalogueDirectory(final AliEnPrincipal user, final String directory) {

		String path = FileSystemUtils.getAbsolutePath(user.getName(), null, directory);

		if (path.endsWith("/"))
			path = path.substring(0, path.length() - 1);

		final LFN lfn = LFNUtils.getLFN(path, true);
		LFN parent = lfn.getParentDir();

		if (!path.endsWith("/"))
			path += "/";

		if (AuthorizationChecker.canRead(lfn, user) && AuthorizationChecker.canWrite(lfn, user)) {

			if (!lfn.exists) {

				if (!lfn.lfn.endsWith("/"))
					lfn.lfn += "/";

				lfn.size = 0;
				lfn.owner = user.getName();
				lfn.gowner = user.getName();
				lfn.type = 'd';
				lfn.perm = "755";

				parent = LFNUtils.ensureDir(parent);

				if (parent == null) {
					logger.log(Level.WARNING, "Parent dir for new directory [" + path + "]  is null for " + lfn.getCanonicalName());
					return null;
				}

				lfn.parentDir = parent;

				final IndexTableEntry ite = CatalogueUtils.getClosestMatch(path);
				if (ite == null) {
					logger.log(Level.WARNING, "Insertion for new directory [" + path + "] failed, ite null: " + lfn.getCanonicalName());
					return null;
				}
				lfn.indexTableEntry = ite;

				lfn.dir = parent.entryId;

				final boolean inserted = LFNUtils.insertLFN(lfn);

				if (!inserted) {
					logger.log(Level.WARNING, "New directory [" + path + "] creation failed. Could not insert this LFN in the catalog : " + lfn);
					return null;
				}
				return LFNUtils.getLFN(path, true);
			}

			return lfn;
		}

		logger.log(Level.WARNING, "New directory [" + path + "] creation failed. Authorization failed.");

		return null;
	}

	/**
	 * @param sourcename
	 * @param user
	 * @return the matching lfns from the catalogue or <code>null</code> if failed to expand path
	 */
	public static List<String> expandPathWildCards(final String sourcename, final AliEnPrincipal user) {
		final List<String> result = new ArrayList<>();

		final int idxStar = sourcename.indexOf('*');
		final int idxQM = sourcename.indexOf('?');

		if (idxStar < 0 && idxQM < 0) {
			result.add(sourcename);
			return result;
		}

		final int minIdx = idxStar >= 0 ? (idxQM >= 0 ? Math.min(idxStar, idxQM) : idxStar) : idxQM;

		final int lastIdx = sourcename.lastIndexOf('/', minIdx);

		final String path;
		final String pattern;

		if (lastIdx < 0) {
			path = "/";
			pattern = sourcename;
		}
		else {
			path = sourcename.substring(0, lastIdx + 1);
			pattern = sourcename.substring(lastIdx + 1);
		}

		try {
			// List all files and folders in the path (e.g. /alice/cern.ch/user/v/vyurchen/)
			final LFNListingfromString listing = Dispatcher.execute(new LFNListingfromString(user, path));

			if (listing != null && listing.getLFNs() != null) {
				// If the pattern is complex (e.g. 01*/*Trig?er*.root - contains many wildcards in different directories)
				// then expand paths recursively
				if (pattern.contains("/")) {
					// Strip the pattern and forget about everything after "/" for now
					final String processedPattern = Format.replace(Format.replace(pattern.substring(0, pattern.indexOf("/")), "*", ".*"), "?", ".");
					final Pattern p = Pattern.compile("^" + processedPattern + "$");
					for (final LFN l : listing.getLFNs()) {
						// Check only directories (because we have "/" in pattern)
						if (l.isDirectory()) {
							final Matcher m = p.matcher(l.getFileName());
							if (m.matches()) {
								final List<String> expandMore = expandPathWildCards(l.getCanonicalName() + pattern.substring(pattern.indexOf("/") + 1), user);
								if (expandMore != null && !expandMore.isEmpty())
									result.addAll(expandMore);
							}
						}
					}
				}
				else {
					// If the pattern contains only one wildcard in the filename - just find a match for it
					final String processedPattern = Format.replace(Format.replace(pattern, "*", ".*"), "?", ".");

					final Pattern p = Pattern.compile("^" + processedPattern + "$");

					for (final LFN l : listing.getLFNs()) {
						final Matcher m = p.matcher(l.getFileName());

						if (m.matches())
							result.add(l.getCanonicalName());
					}
				}
			}
			else
				// Failed to get listing for the directory - probably it doesn't exist
				return null;

			// TODO: This piece of code is here to remind you, that probably FindfromString would show better performance
			// in some cases, so you need to run benchmarks and find out what are those cases

			// if (!pattern.contains("/")) {
			// final LFNListingfromString listing = Dispatcher.execute(new LFNListingfromString(user, path));
			//
			// String processedPattern = Format.replace(Format.replace(pattern, "*", ".*"), "?", ".");
			//
			// final Pattern p = Pattern.compile("^" + processedPattern + "$");
			// System.out.println("p " + p.toString());
			//
			// for (final LFN l : listing.getLFNs()) {
			//
			// System.out.println("l.getFileName() " + l.getFileName());
			// final Matcher m = p.matcher(l.getFileName());
			//
			// if (m.matches())
			// result.add(l.getCanonicalName());
			// }
			// }
			// else {
			// final FindfromString ret = Dispatcher.execute(new FindfromString(user, path, pattern + "$", LFNUtils.FIND_REGEXP | LFNUtils.FIND_INCLUDE_DIRS));
			//
			// if (ret != null) {
			// final Collection<LFN> lfns = ret.getLFNs();
			//
			// if (lfns != null) {
			// for (final LFN l : lfns) {
			// System.out.println("l.getCanonicalName() " + l.getCanonicalName());
			// result.add(l.getCanonicalName());
			// }
			// }
			// }
			// }

		}
		catch (final ServerException se) {
			return null;
		}
		return result;
	}

	// public List<LFN> expandPathWildcards(final LFN source, String sourcename, AliEnPrincipal user, String role, String criteria)
	// {
	// List<LFN> result = null;
	// String[] components = sourcename.split("*");
	// String basename = components[0];
	// for(int i = 1; i < components.length; i++)
	// {
	// String component = components[i];
	// LFN temp = (new LFNfromString(user, role, component, false)).getLFN();
	// if(temp.isDirectory())
	// {
	// result.addAll(expandPathWildcards(temp, component, user, role, criteria));
	// }
	// }
	// return null;
	// }

	/**
	 * @param user
	 * @param path
	 * @param createNonExistentParents
	 * @return the LFN
	 */
	public static LFN createCatalogueDirectory(final AliEnPrincipal user, final String path, final boolean createNonExistentParents) {

		if (createNonExistentParents) {
			String goDown = path;
			if (goDown.endsWith("/"))
				goDown = goDown.substring(0, goDown.length() - 1);
			final ArrayList<String> parents = new ArrayList<>();
			parents.add(goDown);
			while (goDown.lastIndexOf('/') != 0) {
				goDown = goDown.substring(0, goDown.lastIndexOf('/'));
				parents.add(goDown);
			}

			final LinkedList<String> toDo = new LinkedList<>();
			for (final String parent : parents)
				if (LFNUtils.getLFN(parent) == null)
					toDo.add(parent);
			LFN ret = null;
			while (!toDo.isEmpty()) {
				ret = createCatalogueDirectory(user, toDo.getLast());
				toDo.removeLast();
			}
			return ret;
		}

		return createCatalogueDirectory(user, path);
	}

	/**
	 * Get the absolute path, currentDir can be <code>null</code> then currentDir is set to user's home
	 *
	 * @param user
	 * @param currentDirectory
	 * @param cataloguePath
	 * @return absolute path, or <code>null</code> if none could be found
	 */
	public static String getAbsolutePath(final String user, final String currentDirectory, final String cataloguePath) {
		String currentDir = currentDirectory != null ? currentDirectory : UsersHelper.getHomeDir(user);

		if (!currentDir.endsWith("/"))
			currentDir += "/";

		String path = cataloguePath;

		if (path.startsWith("alien://"))
			path = path.substring(8);
		else if (path.startsWith("alien:"))
			path = path.substring(6);

		if (path.indexOf('~') == 0)
			path = UsersHelper.getHomeDir(user) + path.substring(1, path.length());

		if (path.indexOf('/') != 0)
			path = currentDir + path;

		if (path.contains("//")) {
			path = path.replace("///", "/");
			path = path.replace("//", "/");
		}

		if (path.endsWith("/") && path.length() != 1)
			path = path.substring(0, path.lastIndexOf('/'));

		while (path.contains("/./"))
			path = path.replace("/./", "/");

		while (path.contains("/..")) {
			final int pos = path.indexOf("/..") - 1;
			String newpath = path.substring(0, pos);
			newpath = newpath.substring(0, newpath.lastIndexOf('/'));
			if (path.length() > (pos + 3))
				path = newpath + "/" + path.substring(pos + 4);
			else
				path = newpath;
		}

		if (path.endsWith("/."))
			path = path.substring(0, path.length() - 1);

		if (path.endsWith("/.."))
			path = path.substring(0, currentDir.lastIndexOf('/'));

		return path;
	}

	private static final String[] translation = new String[] { "---", "--x", "-w-", "-wx", "r--", "r-x", "rw-", "rwx" };

	/**
	 * Get the type+perm string for LFN
	 *
	 * @param lfn
	 * @return type+perm String e.g. -rwxrwxr-x or drwxr-xr-x
	 */
	public static String getFormatedTypeAndPerm(final LFN lfn) {
		final StringBuilder ret = new StringBuilder(10);

		if (lfn.type != 'f')
			ret.append(lfn.type);
		else
			ret.append('-');

		for (int pos = 0; pos < 3; pos++)
			ret.append(translation[lfn.perm.charAt(pos) - '0']);

		return ret.toString();
	}

	/**
	 * Get the type+perm string for LFN_CSD
	 *
	 * @param lfnc
	 * @return type+perm String e.g. -rwxrwxr-x or drwxr-xr-x
	 */
	public static String getFormatedTypeAndPerm(final LFN_CSD lfnc) {
		LFN l = new LFN("");
		l.perm = lfnc.perm;
		l.type = lfnc.type;

		return getFormatedTypeAndPerm(l);
	}
}
