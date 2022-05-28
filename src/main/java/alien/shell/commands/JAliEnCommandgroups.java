package alien.shell.commands;

import java.util.List;
import java.util.Set;

import joptsimple.OptionException;

/**
 * @author costing
 */
public class JAliEnCommandgroups extends JAliEnBaseCommand {

	@Override
	public void run() {
		final Set<String> roles = commander.getUser().getRoles();
		final String username = commander.getUser().getName();
		final String maingroup = commander.getUser().getDefaultRole();
		commander.printOutln("User: " + username + ", main group: " + maingroup);
		commander.printOut("Member of groups: ");
		for (final String role : roles)
			commander.printOut(role + " ");
		commander.printOutln();
	}

	@Override
	public void printHelp() {
		commander.printOutln();
		commander.printOutln("groups [<username>]");
		commander.printOutln("shows the groups current user is a member of.");
		commander.printOutln();
	}

	@Override
	public boolean canRunWithoutArguments() {
		return true;
	}

	/**
	 * @param commander
	 * @param alArguments
	 * @throws OptionException
	 */
	public JAliEnCommandgroups(final JAliEnCOMMander commander, final List<String> alArguments) throws OptionException {
		super(commander, alArguments);
	}

}
