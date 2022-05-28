package alien.shell.commands;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;

import alien.api.Dispatcher;
import alien.api.ServerException;
import alien.api.catalogue.GetTagValues;
import alien.api.catalogue.GetTags;
import alien.catalogue.FileSystemUtils;
import alien.shell.ErrNo;
import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;

/**
 * Command for displaying metadata information
 *
 * @author costing
 * @since 2020-02-25
 */
public class JAliEnCommandshowTagValue extends JAliEnBaseCommand {

	private ArrayList<String> alPaths = null;

	private Set<String> theseTagsOnly = null;

	private Set<String> theseColumnsOnly = null;

	private boolean bL = false;

	@Override
	public void run() {
		boolean first = true;

		if (alPaths == null || alPaths.size() == 0) {
			commander.setReturnCode(ErrNo.EINVAL, "No paths were indicated");
			return;
		}

		for (final String eachFileName : alPaths) {
			final String absolutePath = FileSystemUtils.getAbsolutePath(commander.user.getName(), commander.getCurrentDirName(), eachFileName);
			final List<String> sources = FileSystemUtils.expandPathWildCards(absolutePath, commander.user);

			if (sources == null || sources.size() == 0) {
				commander.printErrln("Could not resolve this path: " + eachFileName);
				continue;
			}

			for (final String file : sources) {
				if (!first)
					commander.printOutln();

				commander.printOutln(file);
				first = false;

				Set<String> tags = theseTagsOnly;

				if (tags == null || tags.size() == 0) {
					try {
						final GetTags availableTags = Dispatcher.execute(new GetTags(commander.user, file));

						tags = availableTags.getTags();

						if (tags == null || tags.size() == 0) {
							commander.printOutln("  no tags defined for this path");
							continue;
						}
					}
					catch (final ServerException e) {
						commander.setReturnCode(ErrNo.ENODATA, "Could not get the list of tags for " + file + ": " + e.getMessage());

						return;
					}
				}

				if (bL) {
					commander.printOut("  available tags: ");

					boolean firstTag = true;

					for (final String tag : tags) {
						if (!firstTag)
							commander.printOut(", ");

						firstTag = false;

						commander.printOut(tag);
					}

					commander.printOutln();

					for (final String tag : tags) {
						commander.printOut("fileName", file);
						commander.printOut("tagName", tag);
						commander.outNextResult();
					}

					continue;
				}

				for (final String tag : tags) {
					commander.printOutln("  " + tag);

					try {
						final GetTagValues tagValues = Dispatcher.execute(new GetTagValues(commander.user, file, tag, theseColumnsOnly));

						for (final Map.Entry<String, String> tagEntry : tagValues.getTagValues().entrySet()) {
							commander.printOutln("    " + tagEntry.getKey() + "=" + tagEntry.getValue());

							commander.printOut("fileName", file);
							commander.printOut("tagName", tag);

							commander.printOut("column", tagEntry.getKey());
							commander.printOut("value", tagEntry.getValue());

							commander.outNextResult();
						}
					}
					catch (final ServerException e) {
						commander.setReturnCode(ErrNo.ENODATA, "Could not get the columns for " + file + " for tag " + tag + ": " + e.getMessage());

						return;
					}
				}
			}
		}
	}

	/**
	 * printout the help info
	 */
	@Override
	public void printHelp() {
		commander.printOutln();
		commander.printOutln(helpUsage("showtagValue", "[flags] <filename> [<filename>...]"));
		commander.printOutln(helpStartOptions());
		commander.printOutln(helpOption("-t", "restrict to this (comma separated) tag list only (default is to return all available tags)"));
		commander.printOutln(helpOption("-c", "restrict to these (comma separated) list of attributes"));
		commander.printOutln(helpOption("-l", "list available tags only"));
		commander.printOutln();
	}

	/**
	 * cat cannot run without arguments
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
	public JAliEnCommandshowTagValue(final JAliEnCOMMander commander, final List<String> alArguments) throws OptionException {
		super(commander, alArguments);

		try {
			final OptionParser parser = new OptionParser();

			parser.accepts("t").withRequiredArg();
			parser.accepts("c").withRequiredArg();
			parser.accepts("l");

			final OptionSet options = parser.parse(alArguments.toArray(new String[] {}));

			alPaths = new ArrayList<>(options.nonOptionArguments().size());
			alPaths.addAll(optionToString(options.nonOptionArguments()));

			bL = options.has("l");

			if (!bL) {
				if (options.has("t")) {
					final StringTokenizer st = new StringTokenizer(options.valueOf("t").toString(), ",;");

					theseTagsOnly = new HashSet<>();

					while (st.hasMoreTokens()) {
						theseTagsOnly.add(st.nextToken());
					}
				}

				if (options.has("c")) {
					final StringTokenizer st = new StringTokenizer(options.valueOf("c").toString(), ",;");

					theseColumnsOnly = new HashSet<>();

					while (st.hasMoreTokens()) {
						theseColumnsOnly.add(st.nextToken());
					}
				}
			}
		}
		catch (final OptionException e) {
			commander.setReturnCode(ErrNo.EINVAL, e.getMessage());
			setArgumentsOk(false);
		}
	}
}
