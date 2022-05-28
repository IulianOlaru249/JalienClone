package alien.shell.commands;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import alien.catalogue.FileSystemUtils;
import alien.catalogue.LFN;
import alien.catalogue.LFNUtils;
import alien.catalogue.PFN;
import alien.shell.ErrNo;
import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import lazyj.Format;

/**
 *
 */
public class JAliEnCommanddu extends JAliEnBaseCommand {

	private boolean bN = false;
	private boolean bC = false;
	private boolean bS = false;

	private List<String> paths = null;

	private static class DUStats {
		long physicalFiles = 0;
		long physicalOneCopy = 0;
		long physicalTotalSize = 0;
		long physicalReplicas = 0;

		long logicalFiles = 0;
		long logicalSize = 0;

		long subfolders = 0;

		public DUStats() {

		}

		public void addPhysicalFile(final long size, final long replicas) {
			physicalFiles++;
			physicalOneCopy += size;
			physicalTotalSize += size * replicas;
			physicalReplicas += replicas;
		}

		public void addLogicalFile(final long size) {
			logicalFiles++;
			logicalSize += size;
		}

		public void addSubfolder() {
			subfolders++;
		}

		public void addStats(final DUStats other) {
			physicalFiles += other.physicalFiles;
			physicalOneCopy += other.physicalOneCopy;
			physicalTotalSize += other.physicalTotalSize;
			physicalReplicas += other.physicalReplicas;

			logicalFiles += other.logicalFiles;
			logicalSize += other.logicalSize;

			subfolders += other.subfolders;
		}
	}

	private String getSize(final long value) {
		return (bN ? String.valueOf(value) + " bytes" : Format.size(value));
	}

	private void printStats(final String firstLine, final DUStats stats) {
		commander.printOutln(firstLine);

		if (stats.subfolders > 0)
			commander.printOutln("  Folders: " + stats.subfolders);

		commander.printOutln("  Files: " + (stats.logicalFiles + stats.physicalFiles) + " of an apparent size of " + getSize(stats.physicalOneCopy + stats.logicalSize) + ", of which:");

		if (stats.physicalFiles > 0) {
			commander.printOut("  - physical files: " + stats.physicalFiles + " files of " + getSize(stats.physicalTotalSize));
			commander.printOut(" with " + stats.physicalReplicas + " replicas");

			commander.printOut(" (avg " + Format.point((double) stats.physicalReplicas / stats.physicalFiles) + " replicas/file)");

			commander.printOutln(", size of one replica: " + getSize(stats.physicalOneCopy));
		}

		if (stats.logicalFiles > 0)
			commander.printOutln("  - archive members : " + stats.logicalFiles + " files of " + getSize(stats.logicalSize));

		// and now set the return values for JSON export
		commander.outNextResult();
		commander.printOut("entry", firstLine);
		commander.printOut("folders", String.valueOf(stats.subfolders));
		commander.printOut("physical_files_count", String.valueOf(stats.physicalFiles));
		commander.printOut("physical_files_size", String.valueOf(stats.physicalTotalSize));
		commander.printOut("physical_files_replicas", String.valueOf(stats.physicalReplicas));
		commander.printOut("physical_files_one_replica_size", String.valueOf(stats.physicalOneCopy));
		commander.printOut("logical_files_count", String.valueOf(stats.logicalFiles));
		commander.printOut("logical_files_size", String.valueOf(stats.logicalSize));
	}

	@Override
	public void run() {
		if (paths == null || paths.size() == 0)
			return;

		final DUStats summary = new DUStats();

		final List<String> pathsToRunOn = new ArrayList<>();

		for (final String path : paths) {
			final String absolutePath = FileSystemUtils.getAbsolutePath(commander.user.getName(), commander.getCurrentDirName(), path);

			final List<String> expandedPaths = FileSystemUtils.expandPathWildCards(absolutePath, commander.user);

			if (expandedPaths == null || expandedPaths.isEmpty()) {
				commander.setReturnCode(ErrNo.ENOENT, path);
				return;
			}

			for (final String expandedPath : expandedPaths)
				if (!pathsToRunOn.contains(expandedPath))
					pathsToRunOn.add(expandedPath);
		}

		for (final String path : pathsToRunOn) {
			final DUStats stats = new DUStats();

			final Collection<LFN> lfns = commander.c_api.find(path, "*", null, LFNUtils.FIND_INCLUDE_DIRS | LFNUtils.FIND_NO_SORT, null, null, 10000000);

			if (lfns != null) {
				for (final LFN l : lfns) {
					if (l.isDirectory()) {
						if (!l.getCanonicalName().equals(path))
							stats.addSubfolder();
					}
					else if (l.isCollection() && bC)
						stats.addLogicalFile(l.getSize());
					else if (l.isFile()) {
						final Set<PFN> pfns = commander.c_api.getPFNs(l.guid.toString());

						if (pfns != null && pfns.size() > 0) {
							boolean logicalFile = false;
							int physicalReplicas = 0;

							for (final PFN p : pfns) {
								if (p.pfn.startsWith("guid://"))
									logicalFile = true;
								else
									physicalReplicas++;
							}

							if (logicalFile)
								stats.addLogicalFile(l.getSize());

							if (physicalReplicas > 0)
								stats.addPhysicalFile(l.getSize(), physicalReplicas);
						}
					}
				}

				if (!bS)
					printStats(path, stats);
				else
					summary.addStats(stats);
			}
			else
				commander.printErrln("Could not get the usage of " + path);
		}

		if (bS)
			printStats("Summary of " + paths.size() + " paths", summary);
	}

	@Override
	public void printHelp() {
		commander.printOutln("Gives the disk space usage of one or more directories");
		commander.printOutln(helpUsage("du", "[-ncs] <path>"));
		commander.printOutln(helpStartOptions());
		commander.printOutln(helpOption("-n", "Print raw numbers in machine readable format"));
		commander.printOutln(helpOption("-c", "Include collections in the summary information"));
		commander.printOutln(helpOption("-s", "Print a summary of all parameters"));
	}

	@Override
	public boolean canRunWithoutArguments() {
		return false;
	}

	/**
	 * @param commander
	 * @param alArguments
	 * @throws OptionException
	 */
	public JAliEnCommanddu(final JAliEnCOMMander commander, final List<String> alArguments) throws OptionException {
		super(commander, alArguments);

		try {
			final OptionParser parser = new OptionParser();
			parser.accepts("n");
			parser.accepts("c");
			parser.accepts("s");

			final OptionSet options = parser.parse(alArguments.toArray(new String[] {}));

			paths = optionToString(options.nonOptionArguments());

			if (paths.size() < 1)
				return;

			bN = options.has("n");
			bC = options.has("c");
			bS = options.has("s");
		}
		catch (final OptionException e) {
			commander.setReturnCode(ErrNo.EINVAL, e.getMessage());
			setArgumentsOk(false);
		}
	}
}
