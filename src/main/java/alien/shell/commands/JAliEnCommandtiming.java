package alien.shell.commands;

import java.util.List;

import joptsimple.OptionException;
import lazyj.Utils;

/**
 * @author costing
 * @since 2020-09-24
 */
public class JAliEnCommandtiming extends JAliEnBaseCommand {

	private String option = null;

	@Override
	public void run() {
		if (option != null) {
			switch (option) {
				case "on":
					commander.setTiming(true);
					break;
				case "off":
					commander.setTiming(false);
					break;
				default:
					commander.setTiming(Utils.stringToBool(option, commander.getTiming()));
			}
		}

		commander.printOutln("Command timing is " + (commander.getTiming() ? "enabled" : "disabled"));
		commander.printOut("timing", String.valueOf(commander.getTiming()));
	}

	/**
	 * printout the help info
	 */
	@Override
	public void printHelp() {
		commander.printOutln();
		commander.printOutln(helpUsage("timing", "[on|off]"));
		commander.printOutln(helpStartOptions());
		commander.printOutln(helpOption("return server side timing information"));
		commander.printOutln();
	}

	/**
	 * cat cannot run without arguments
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
	 * @throws OptionException
	 */
	public JAliEnCommandtiming(final JAliEnCOMMander commander, final List<String> alArguments) throws OptionException {
		super(commander, alArguments);

		if (alArguments.size() > 0)
			option = alArguments.get(0).trim().toLowerCase();
	}
}
