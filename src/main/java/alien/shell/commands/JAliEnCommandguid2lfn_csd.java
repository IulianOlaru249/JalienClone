package alien.shell.commands;

import java.util.List;

import alien.catalogue.LFN_CSD;
import alien.shell.ErrNo;
import joptsimple.OptionException;

/**
 * @author mmmartin
 * @since November 27, 2018
 */
public class JAliEnCommandguid2lfn_csd extends JAliEnBaseCommand {

	/**
	 * entry the call is executed on, either representing a LFNCSD
	 */
	private String guidName = null;

	/**
	 * execute the lfn2guid
	 */
	@Override
	public void run() {
		final LFN_CSD lfnc = commander.c_api.guid2lfncsd(guidName);

		if (lfnc == null)
			commander.setReturnCode(ErrNo.ENXIO, "Could not get the UUID [" + guidName + "].");
		else if (lfnc.exists)
			commander.printOutln("LFN: " + lfnc.getCanonicalName());
		else
			commander.setReturnCode(ErrNo.ENOENT, "No LFNs are associated to this UUID [" + guidName + "].");
	}

	/**
	 * printout the help info
	 */
	@Override
	public void printHelp() {
		commander.printOutln();
		commander.printOutln(helpUsage("guid2lfn_csd", "<UUID>"));
		commander.printOutln();
	}

	/**
	 * guid2lfn cannot run without arguments
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
	public JAliEnCommandguid2lfn_csd(final JAliEnCOMMander commander, final List<String> alArguments) throws OptionException {
		super(commander, alArguments);

		if (alArguments.size() != 1)
			throw new JAliEnCommandException();

		guidName = alArguments.get(0);

	}

}
