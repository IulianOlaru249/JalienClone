package alien.shell.commands;

import java.util.List;
import java.util.Set;

import alien.catalogue.FileSystemUtils;
import alien.catalogue.GUID;
import alien.catalogue.GUIDUtils;
import alien.catalogue.LFN;
import alien.catalogue.PFN;
import alien.se.SE;
import alien.shell.ErrNo;
import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;

/**
 * @author ron
 * @since June 4, 2011
 */
public class JAliEnCommandwhereis extends JAliEnBaseCommand {

	/**
	 * marker for -g argument
	 */
	private boolean bG = false;

	/**
	 * marker for -r argument
	 */
	private boolean bR = false;

	/**
	 * marker for the -l argument: try to locate the archive LFN to which a file belongs
	 */
	private boolean bL = false;

	/**
	 * entry the call is executed on, either representing a LFN or a GUID
	 */
	private String lfnOrGuid = null;

	/**
	 * execute the whereis
	 */
	@Override
	public void run() {

		String guid = null;

		if (bG) {
			if (GUIDUtils.isValidGUID(lfnOrGuid))
				guid = lfnOrGuid;
			else {
				commander.setReturnCode(ErrNo.EINVAL, "This is not a valid GUID: " + lfnOrGuid);
				return;
			}
		}
		else {
			final LFN lfn = commander.c_api.getLFN(FileSystemUtils.getAbsolutePath(commander.user.getName(), commander.getCurrentDirName(), lfnOrGuid));

			if (lfn != null && lfn.isFile() && lfn.guid != null)
				guid = lfn.guid.toString();
			else if (GUIDUtils.isValidGUID(lfnOrGuid)) {
				bG = true;
				guid = lfnOrGuid;
			}
			else if (lfn != null && !lfn.isFile()) {
				commander.setReturnCode(ErrNo.EINVAL, "The path you indicated is not a file: " + lfnOrGuid);
				return;
			}
		}

		if (guid != null) {
			Set<PFN> pfns = commander.c_api.getPFNs(guid);

			if (pfns != null && pfns.size() > 0) {
				String zipMember = null;

				LFN archiveContainer = null;

				if (bR) {
					String archiveGUID = null;

					for (final PFN p : pfns)
						if (p.pfn.startsWith("guid://")) {
							archiveGUID = p.pfn.substring(8, 44);

							final int idxQM = p.pfn.lastIndexOf("?ZIP");

							if (idxQM >= 0)
								zipMember = p.pfn.substring(idxQM);
						}

					if (archiveGUID != null) {
						pfns = commander.c_api.getPFNs(archiveGUID);

						if (pfns == null) {
							commander.setReturnCode(ErrNo.ENOENT, "Archive with GUID " + archiveGUID + " doesn't exist");
							return;
						}

						if (bL) {
							final GUID aGUID = commander.c_api.getGUID(archiveGUID, false, true);

							if (aGUID != null && aGUID.getLFNs() != null && aGUID.getLFNs().size() > 0)
								archiveContainer = aGUID.getLFNs().iterator().next();
							else
								commander.printErrln("Could not resolve the LFN for the archive guid " + archiveGUID);
						}

						if (pfns.size() == 0) {
							commander.setReturnCode(ErrNo.EBADFD, "Archive with GUID " + archiveGUID + " doesn't have any replicas, this file cannot be used");
							return;
						}
					}
					else
						bR = false; // disable the archive lookup flag because this file is not member of an archive
				}

				if (bG)
					commander.printOutln("the GUID " + guid + " is in" + (bR ? "side a ZIP archive" : ""));
				else
					commander.printOutln("the file " + lfnOrGuid.substring(lfnOrGuid.lastIndexOf("/") + 1, lfnOrGuid.length()) + " is in" + (bR ? "side a ZIP archive" : ""));

				if (bR)
					if (bL) {
						if (archiveContainer != null)
							commander.printOutln("    archive LFN: " + archiveContainer.getCanonicalName());
					}
					else
						commander.printOutln("    pass `-l` to whereis to try to resolve the archive LFN (slow, expensive operation!)");

				commander.printOutln();

				for (final PFN pfn : pfns) {
					String se;

					if (pfn.seNumber > 0) {
						commander.printOut("seNumber", String.valueOf(pfn.seNumber));

						final SE theSE = commander.c_api.getSE(pfn.seNumber);

						if (theSE != null) {
							se = "SE => " + theSE.seName;

							commander.printOut("seName", theSE.seName);
						}
						else {
							se = "SE #" + pfn.seNumber + " no longer exists";

							commander.printOut("seName", "Unknown");
						}
					}
					else {
						se = "ZIP archive member";

						commander.printOut("seName", "ArchiveMember");
					}

					commander.printOutln("\t " + padRight(se, 30) + " pfn => " + pfn.pfn + (zipMember != null ? zipMember : "") + "\n");

					commander.printOut("pfn", pfn.pfn + (zipMember != null ? zipMember : ""));

					commander.outNextResult();
				}
			}
			else if (pfns == null)
				commander.setReturnCode(ErrNo.ENOENT, "GUID " + guid + " does not exist in the catalogue");
			else
				commander.setReturnCode(ErrNo.ENOLINK, "GUID " + guid + " has no replicas, this is a lost file");
		}
		else
			commander.setReturnCode(ErrNo.ENOENT, lfnOrGuid);

	}

	/**
	 * printout the help info
	 */
	@Override
	public void printHelp() {
		commander.printOutln();
		commander.printOutln(helpUsage("whereis", "[-options] [<filename>]"));
		commander.printOutln(helpStartOptions());
		commander.printOutln(helpOption("-g", "use the lfn as guid"));
		commander.printOutln(helpOption("-r", "resolve links (do not give back pointers to zip archives)"));
		commander.printOutln(helpOption("-l", "lookup the LFN of the ZIP archive (slow and expensive IO operation, only use it sparingly!)"));
		commander.printOutln();
	}

	/**
	 * whereis cannot run without arguments
	 *
	 * @return <code>false</code>
	 */
	@Override
	public boolean canRunWithoutArguments() {
		return false;
	}

	/**
	 * Constructor needed for the command factory in JAliEnCOMMander
	 *
	 * @param commander
	 *
	 * @param alArguments
	 *            the arguments of the command
	 * @throws OptionException
	 */
	public JAliEnCommandwhereis(final JAliEnCOMMander commander, final List<String> alArguments) throws OptionException {
		super(commander, alArguments);
		try {
			final OptionParser parser = new OptionParser();
			parser.accepts("g");
			parser.accepts("r");
			parser.accepts("l");

			final OptionSet options = parser.parse(alArguments.toArray(new String[] {}));

			bG = options.has("g");
			bR = options.has("r");
			bL = options.has("l");

			if (options.nonOptionArguments().iterator().hasNext())
				lfnOrGuid = options.nonOptionArguments().iterator().next().toString();
			else
				setArgumentsOk(false);
		}
		catch (final OptionException e) {
			commander.setReturnCode(ErrNo.EINVAL, e.getMessage());
			setArgumentsOk(false);
		}
	}
}
