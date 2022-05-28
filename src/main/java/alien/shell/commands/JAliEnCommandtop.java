package alien.shell.commands;

import java.util.List;

import alien.shell.ErrNo;
import joptsimple.OptionException;

// TODO : implement top command

/**
 *
 */
public class JAliEnCommandtop extends JAliEnBaseCommand {
	@Override
	public void run() {
		// final String username = commander.user.getName();

		// TODO implement this
		commander.setReturnCode(ErrNo.ENOSYS, "not implemented yet");
	}

	@Override
	public void printHelp() {
		commander.printOutln();
		commander.printOutln(helpUsage("top", ""));
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
	public JAliEnCommandtop(final JAliEnCOMMander commander, final List<String> alArguments) throws OptionException {
		super(commander, alArguments);
	}
}
