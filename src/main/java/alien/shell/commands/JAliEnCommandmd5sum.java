package alien.shell.commands;

import java.util.ArrayList;
import java.util.List;

import alien.catalogue.FileSystemUtils;
import alien.catalogue.GUID;
import alien.catalogue.GUIDUtils;
import alien.catalogue.LFN;
import alien.shell.ErrNo;
import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;

/**
 * @author costing
 *
 */
public class JAliEnCommandmd5sum extends JAliEnBaseCommand {
	private ArrayList<String> alPaths = null;

	@Override
	public void run() {
		for (final String lfnName : this.alPaths) {
			final LFN lfn = commander.c_api.getLFN(FileSystemUtils.getAbsolutePath(commander.user.getName(), commander.getCurrentDirName(), lfnName));
			if (lfn == null) {
				if (GUIDUtils.isValidGUID(lfnName)) {
					final GUID g = commander.c_api.getGUID(lfnName);

					if (g != null)
						if (g.md5 != null && g.md5.length() > 0)
							commander.printOutln(g.md5 + "\t" + lfnName);
						else
							commander.setReturnCode(ErrNo.ENODATA, "GUID " + lfnName + " doesn't have an associated MD5 checksum");
					else
						commander.setReturnCode(ErrNo.ENOENT, lfnName);
				}
				else
					commander.setReturnCode(ErrNo.ENOENT, lfnName);
			}
			else if (lfn.md5 != null && lfn.md5.length() > 0)
				commander.printOutln(lfn.md5 + "\t" + lfnName);
			else if (!lfn.isFile())
				commander.setReturnCode(ErrNo.EINVAL, "This entry is not a file: " + lfnName);
			else
				commander.setReturnCode(ErrNo.ENODATA, "This file doesn't have a valid associated MD5 checksum: " + lfnName);
		}
	}

	@Override
	public void printHelp() {
		commander.printOutln();
		commander.printOutln(helpUsage("md5sum", "<filename1> [<or guid>] ..."));
		commander.printOutln();
	}

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
	public JAliEnCommandmd5sum(final JAliEnCOMMander commander, final List<String> alArguments) throws OptionException {
		super(commander, alArguments);
		try {
			final OptionParser parser = new OptionParser();
			final OptionSet options = parser.parse(alArguments.toArray(new String[] {}));

			alPaths = new ArrayList<>(options.nonOptionArguments().size());
			alPaths.addAll(optionToString(options.nonOptionArguments()));
		}
		catch (final OptionException e) {
			commander.setReturnCode(ErrNo.EINVAL, e.getMessage());
			setArgumentsOk(false);
		}
	}
}
