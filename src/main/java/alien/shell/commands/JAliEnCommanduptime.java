package alien.shell.commands;

import java.util.List;
import java.util.Map;

import alien.api.taskQueue.GetUptime.UserStats;
import alien.api.taskQueue.TaskQueueApiUtils;
import joptsimple.OptionException;

/**
 * @author ron
 * @since Oct 27, 2011
 */
public class JAliEnCommanduptime extends JAliEnBaseCommand {

	@Override
	public void run() {
		final Map<String, UserStats> stats = TaskQueueApiUtils.getUptime();

		if (stats == null)
			return;

		final UserStats totals = new UserStats();

		for (final UserStats u : stats.values())
			totals.add(u);

		commander.printOut("running jobs", " " + totals.runningJobs);
		commander.printOut("waiting jobs", " " + totals.waitingJobs);
		commander.printOut("active users", " " + stats.size());

		commander.printOutln(totals.runningJobs + " running jobs, " + totals.waitingJobs + " waiting jobs, " + stats.size() + " active users");
	}

	/**
	 * printout the help info
	 */
	@Override
	public void printHelp() {
		commander.printOutln();
		commander.printOutln(helpUsage("uptime", ""));
		commander.printOutln(helpStartOptions());
		commander.printOutln();
	}

	/**
	 * mkdir cannot run without arguments
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
	public JAliEnCommanduptime(final JAliEnCOMMander commander, final List<String> alArguments) throws OptionException {
		super(commander, alArguments);

	}
}
