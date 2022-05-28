package alien.shell.commands;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;

import alien.catalogue.FileSystemUtils;
import alien.catalogue.GUIDUtils;
import alien.catalogue.LFN;
import alien.shell.ErrNo;
import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;

/**
 * @author costing
 * @since 2018-08-15
 */
public class JAliEnCommanduuid extends JAliEnBaseCommand {
	private ArrayList<String> alPaths = null;

	@Override
	public void run() {
		for (final String lfnName : this.alPaths) {
			final LFN lfn = commander.c_api.getLFN(FileSystemUtils.getAbsolutePath(commander.user.getName(), commander.getCurrentDirName(), lfnName));

			UUID u = null;

			if (lfn == null) {
				try {
					u = UUID.fromString(lfnName);
				}
				catch (@SuppressWarnings("unused") final Throwable t) {
					commander.setReturnCode(ErrNo.ENOENT, "File does not exist: " + lfnName);
				}
			}
			else {
				if (lfn.guid != null)
					u = lfn.guid;
				else {
					if (lfn.isDirectory())
						commander.setReturnCode(ErrNo.EISDIR, lfnName);
					else
						commander.setReturnCode(ErrNo.EINVAL, lfnName + " is of type " + lfn.type);
				}
			}

			if (u != null) {
				if (u.version() == 1) {
					final long timestamp = GUIDUtils.epochTime(u);
					final String macAddr = GUIDUtils.getMacAddr(u);
					commander.printOutln(lfnName + " : created on " + (new Date(timestamp)) + " (" + timestamp + ") by " + macAddr);
					commander.printOut("mtime", String.valueOf(timestamp));
					commander.printOut("mac", macAddr);
				}
				else
					commander.setReturnCode(ErrNo.EINVAL, "UUID version " + u.version() + " is not supported (not created by us)");
			}
			else
				commander.setReturnCode(ErrNo.ENOENT, lfnName + " is neither an UUID nor a LFN");
		}
	}

	@Override
	public void printHelp() {
		commander.printOutln();
		commander.printOutln(helpUsage("uuid", "<uuid|filename> [<uuid|filename> ...]"));
		commander.printOutln();
		commander.printOutln(helpParameter("Decode v1 UUIDs and display the interesting bits"));
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
	public JAliEnCommanduuid(final JAliEnCOMMander commander, final List<String> alArguments) throws OptionException {
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
