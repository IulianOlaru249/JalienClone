package alien.shell.commands;

import java.util.List;

import alien.catalogue.FileSystemUtils;
import alien.catalogue.LFN;
import alien.shell.ErrNo;
import joptsimple.OptionException;

/**
 * @author ron
 * @since June 4, 2011
 */
public class JAliEnCommandlfn2guid extends JAliEnBaseCommand {

	/**
	 * entry the call is executed on, either representing a LFN
	 */
	private String lfnName = null;

	/**
	 * execute the lfn2guid
	 */
	@Override
	public void run() {
		final LFN lfn = commander.c_api.getLFN(FileSystemUtils.getAbsolutePath(commander.user.getName(), commander.getCurrentDirName(), lfnName));

		if (lfn == null)
			commander.setReturnCode(ErrNo.ENOENT, lfnName);
		else {
			commander.printOut("lfn", lfn.getCanonicalName());
			commander.printOut("type", String.valueOf(lfn.type));

			if (lfn.isDirectory())
				commander.setReturnCode(ErrNo.EISDIR, lfn.getCanonicalName());
			else if (lfn.guid != null) {
				commander.printOutln(padRight(lfn.getCanonicalName(), 80) + lfn.guid);

				commander.printOut("guid", String.valueOf(lfn.guid));
			}
			else
				commander.setReturnCode(ErrNo.ENODATA, "Could not get the GUID for [" + lfn.getCanonicalName() + "].");
		}
	}

	/**
	 * printout the help info
	 */
	@Override
	public void printHelp() {
		commander.printOutln();
		commander.printOutln(helpUsage("lfn2guid", "<filename>"));
		commander.printOutln();
	}

	/**
	 * lfn2guid cannot run without arguments
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
	public JAliEnCommandlfn2guid(final JAliEnCOMMander commander, final List<String> alArguments) throws OptionException {
		super(commander, alArguments);

		if (alArguments.size() != 1)
			return;

		lfnName = alArguments.get(0);

	}

}
