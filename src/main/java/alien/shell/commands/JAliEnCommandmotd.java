package alien.shell.commands;

import java.util.List;

/**
 * @author ron
 *
 */
public class JAliEnCommandmotd extends JAliEnBaseCommand {

	/**
	 * @param commander
	 * @param alArguments
	 */
	public JAliEnCommandmotd(final JAliEnCOMMander commander, final List<String> alArguments) {
		super(commander, alArguments);
	}

	@Override
	public void run() {
		commander.printOutln();
		commander.printOutln("AliEn Service Message: All is well in the Grid world. Enjoy!");
		commander.printOutln();
		commander.printOutln("Project links:");
		commander.printOutln("\tDocumentation: https://jalien.docs.cern.ch/");
		commander.printOutln("\tBug reporting: https://alice.its.cern.ch/jira/projects/JAL/");
		commander.printOutln("\tOperational issues: alice-analysis-operations@cern.ch");
	}

	@Override
	public void printHelp() {
		commander.printOut(helpUsage("motd", "Message of the day"));
	}

	@Override
	public boolean canRunWithoutArguments() {
		return true;
	}

}
