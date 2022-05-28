package alien.shell.commands;

import java.util.List;

/**
 * @author ron
 * @since August 26, 2011
 */
public class JAliEnCommandpwd extends JAliEnBaseCommand {

	/**
	 * execute the pwd
	 */
	@Override
	public void run() {
		String ret = "";
		ret += commander.curDir.getCanonicalName();
		logger.info("PWD line : " + ret);
		commander.printOut("pwd", commander.curDir.getCanonicalName());
		commander.printOutln(ret);
	}

	/**
	 * printout the help info
	 */
	@Override
	public void printHelp() {
		// ignore
	}

	/**
	 * get cannot run without arguments
	 *
	 * @return <code>false</code>
	 */
	@Override
	public boolean canRunWithoutArguments() {
		return true;
	}

	/**
	 * Constructor needed for the command factory in commander
	 *
	 * @param commander
	 *
	 * @param alArguments
	 *            the arguments of the command
	 */
	public JAliEnCommandpwd(final JAliEnCOMMander commander, final List<String> alArguments) {
		super(commander, alArguments);

	}
}
