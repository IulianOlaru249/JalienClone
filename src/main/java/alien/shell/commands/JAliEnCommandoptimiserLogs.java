package alien.shell.commands;

import java.util.ArrayList;
import java.util.List;

import alien.api.Dispatcher;
import alien.api.ServerException;
import alien.api.catalogue.OptimiserLogs;
import alien.shell.ErrNo;
import joptsimple.OptionParser;
import joptsimple.OptionSet;

/**
 * @author Marta
 * @since May 5, 2021
 */
public class JAliEnCommandoptimiserLogs extends JAliEnBaseCommand {

	private final List<String> classes;
	private boolean verbose;
	private boolean listClasses;
	private int frequency;

	@Override
	public void run() {
		try {
			final OptimiserLogs optimiserLogs = Dispatcher.execute(new OptimiserLogs(classes, frequency, verbose, listClasses));

			commander.printOutln(optimiserLogs.getLogOutput().trim());
		}
		catch (final ServerException e) {
			commander.setReturnCode(ErrNo.ENODATA, "Could not get the optimiser db logs from optimiserLogs command : " + e.getMessage());
			return;
		}

	}

	/**
	 * @return the arguments as a String array
	 */
	public String[] getArgs() {
		return alArguments.size() > 1 ? alArguments.subList(1, alArguments.size()).toArray(new String[0]) : null;
	}

	/**
	 * printout the help info
	 */
	@Override
	public void printHelp() {
		commander.printOutln();
		commander.printOutln("Usage: lastOptimiserLog [-l] [-v] [-f <frequency>] <classnames or metanames>");
		commander.printOutln();
		commander.printOutln(helpParameter("Gets the last log from the optimiser"));
		commander.printOutln(helpParameter("-v : Verbose, displays the frequency, last run timestamp and log"));
		commander.printOutln(helpParameter("-l : List the class names that match a query"));
		commander.printOutln(helpParameter("-f <value> : Frequency, in seconds"));
		commander.printOutln();
	}

	/**
	 * Constructor needed for the command factory in commander
	 *
	 * @param commander
	 *
	 * @param alArguments
	 *            the arguments of the command
	 */
	public JAliEnCommandoptimiserLogs(final JAliEnCOMMander commander, final List<String> alArguments) {
		super(commander, alArguments);

		verbose = false;
		frequency = 0;

		final OptionParser parser = new OptionParser();
		parser.accepts("f").withRequiredArg().ofType(Integer.class);
		parser.accepts("v");
		parser.accepts("l");

		final OptionSet options = parser.parse(alArguments.toArray(new String[] {}));

		if (options.has("v"))
			verbose = true;

		if (options.has("l"))
			listClasses = true;

		if (options.has("f")) {
			frequency = ((Integer) options.valueOf("f")).intValue();
			frequency = frequency > -1 ? frequency * 1000 : frequency;
		}

		final List<String> params = optionToString(options.nonOptionArguments());

		final ArrayList<String> allowedKeyWords = new ArrayList<>();
		allowedKeyWords.add("ResyncLDAP.*");
		allowedKeyWords.add("resyncLDAP");
		this.classes = params;
		for (final String keyword : allowedKeyWords) {
			if (classes.contains(keyword))
				replaceKeyWord(keyword, "ResyncLDAP");
		}
	}

	private void replaceKeyWord(final String keyword, final String substitute) {
		this.classes.remove(keyword);
		this.classes.add(substitute);
	}

	@Override
	public boolean canRunWithoutArguments() {
		return true;
	}
}
