package alien.shell.commands;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import alien.shell.ErrNo;

/**
 *
 */
public class JAliEnCommandgroupmembers extends JAliEnBaseCommand {
	private String group;

	@Override
	public void run() {
		if (this.group == null || this.group.isEmpty()) {
			commander.setReturnCode(ErrNo.EINVAL, "No group name passed");
			return;
		}

		final Set<String> users = commander.q_api.getGroupMembers(this.group);

		if (users == null || users.isEmpty()) {
			commander.setReturnCode(ErrNo.ENOENT, "Group " + this.group + " does not exist");
			return;
		}

		final List<String> sortedUsers = new ArrayList<>(users);
		Collections.sort(sortedUsers);

		commander.printOut("Members of " + this.group + ": " + String.join(" ", sortedUsers));

		commander.printOutln();
	}

	@Override
	public void printHelp() {
		commander.printOutln("Show the accounts that can take the given role name");
		commander.printOutln(helpUsage("groupmembers", "<group name>"));
		commander.printOutln();
	}

	@Override
	public boolean canRunWithoutArguments() {
		return false;
	}

	/**
	 * @param commander
	 * @param alArguments
	 */
	public JAliEnCommandgroupmembers(final JAliEnCOMMander commander, final List<String> alArguments) {
		super(commander, alArguments);
		if (alArguments.size() == 1)
			this.group = alArguments.get(0);
	}

}
