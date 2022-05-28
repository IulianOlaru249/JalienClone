package alien.shell.commands;

import java.util.ArrayList;
import java.util.List;

import alien.catalogue.FileSystemUtils;
import alien.catalogue.LFN;
import alien.shell.ErrNo;
import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import utils.ExpireTime;

/**
 * @author ibrinzoi
 * @since 2021-01-19
 */
public class JAliEnCommandlfnexpiretime extends JAliEnBaseCommand {

	List<String> files;
	ExpireTime expireTime;
	boolean extend;
	boolean remove;

	@Override
	public void run() {
		final LFN currentDir = commander.getCurrentDir();

		final List<String> absolutePaths = new ArrayList<>();

		for (final String file : files) {
			final String absolutePath = FileSystemUtils.getAbsolutePath(commander.user.getName(),
					currentDir != null ? currentDir.getCanonicalName() : null, file);

			final String expireTimeTargets = FileSystemUtils.expandPathWildCards(absolutePath, commander.user).get(0);

			if (expireTimeTargets == null || expireTimeTargets.isEmpty()) {
				commander.setReturnCode(ErrNo.ENOENT, file);
				return;
			}

			absolutePaths.add(absolutePath);
		}

		if (this.remove) {
			commander.printOutln("Removing expiration time");
			commander.c_api.setLFNExpireTime(absolutePaths, null, false);
		}
		else {
			if (extend) {
				if (expireTime.toDays() == 0) {
					commander.printOutln("No amount of time specified for extension");
					return;
				}

				commander.printOutln("Extending the expiration time with " + extend);
			}
			else {
				if (expireTime.toDays() <= 0)
					commander.printOutln("Marking the targets as expired");
				else
					commander.printOutln("Setting the expiration time to " + expireTime + " from now");
			}

			commander.c_api.setLFNExpireTime(absolutePaths, expireTime, extend);
		}
	}

	/**
	 * printout the help info
	 */
	@Override
	public void printHelp() {
		commander.printOutln();
		commander.printOutln(helpUsage("lfnexpiretime", "[-options] [<file>]"));
		commander.printOutln(helpStartOptions());
		commander.printOutln(helpOption("-r", "removes the expire time set for an LFN"));
		commander.printOutln(helpOption("-a", "add a new expire time for the given LFN"));
		commander.printOutln(helpOption("-e", "extends the current expire time for the given LFN"));
		commander.printOutln(helpOption("-d <number>", "specifies the number of days in the expire time"));
		commander.printOutln(helpOption("-w <number>", "specifies the number of weeks in the expire time"));
		commander.printOutln(helpOption("-m <number>", "specifies the number of months in the expire time"));
		commander.printOutln(helpOption("-y <number>", "specifies the number of years in the expire time"));
		commander.printOutln();
	}

	/**
	 * This command must have at least an argument
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
	public JAliEnCommandlfnexpiretime(final JAliEnCOMMander commander, final List<String> alArguments) throws OptionException {
		super(commander, alArguments);
		try {
			final OptionParser parser = new OptionParser();

			parser.accepts("a");
			parser.accepts("e");
			parser.accepts("r");

			parser.accepts("d").withRequiredArg().describedAs("Number of days").ofType(Integer.class);
			parser.accepts("w").withRequiredArg().describedAs("Number of weeks").ofType(Integer.class);
			parser.accepts("m").withRequiredArg().describedAs("Number of months").ofType(Integer.class);
			parser.accepts("y").withRequiredArg().describedAs("Number of years").ofType(Integer.class);

			final OptionSet options = parser.parse(alArguments.toArray(new String[] {}));

			this.files = optionToString(options.nonOptionArguments());

			this.expireTime = new ExpireTime();

			if (options.has("a")) {
				this.extend = false;
			}

			if (options.has("e")) {
				this.extend = true;
			}

			if (options.has("r")) {
				this.remove = true;
			}

			if (options.hasArgument("d")) {
				this.expireTime.setDays(((Integer) options.valueOf("d")).intValue());
			}

			if (options.hasArgument("w")) {
				this.expireTime.setWeeks(((Integer) options.valueOf("w")).intValue());
			}

			if (options.hasArgument("m")) {
				this.expireTime.setMonths(((Integer) options.valueOf("m")).intValue());
			}

			if (options.hasArgument("y")) {
				this.expireTime.setYears(((Integer) options.valueOf("y")).intValue());
			}
		}
		catch (final OptionException e) {
			commander.setReturnCode(ErrNo.EINVAL, e.getMessage());
			setArgumentsOk(false);
		}
	}

}