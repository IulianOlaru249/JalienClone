package alien.shell.commands;

import java.util.ArrayList;
import java.util.List;

import alien.api.Dispatcher;
import alien.api.ServerException;
import alien.api.catalogue.RemoveLFNCSDfromString;
import alien.catalogue.FileSystemUtils;
import alien.shell.ErrNo;
import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;

/**
 * @author mmmartin
 * @since November 27, 2018
 */
public class JAliEnCommandrm_csd extends JAliEnBaseCommand {
	/**
	 * Variable for -r "Recursive" flag
	 */
	boolean bR = false;

	/**
	 * Variable for -v "Verbose" flag
	 */
	boolean bV = false;

	private List<String> alPaths = null;

	@Override
	public void run() {
		final List<String> expandedPaths = new ArrayList<>(alPaths.size());

		for (final String path : alPaths) {
			final String absolutePath = FileSystemUtils.getAbsolutePath(commander.user.getName(), commander.getCurrentDirName(), path);
			final List<String> sources = FileSystemUtils.expandPathWildCards(absolutePath, commander.user);

			if (sources != null && !sources.isEmpty())
				expandedPaths.addAll(sources);
			else
				commander.setReturnCode(ErrNo.ENOENT, path);
		}

		for (final String sPath : expandedPaths) {
			final RemoveLFNCSDfromString rlfn = new RemoveLFNCSDfromString(commander.getUser(), sPath, bR);

			try {
				final RemoveLFNCSDfromString a = Dispatcher.execute(rlfn);

				if (!a.wasRemoved())
					commander.setReturnCode(ErrNo.EIO, "Failed to remove: " + sPath);
			}
			catch (final ServerException e) {
				e.getCause().printStackTrace();
				commander.setReturnCode(ErrNo.EIO, "Failed to remove: " + sPath);
			}
		}
	}

	/**
	 * printout the help info
	 */
	@Override
	public void printHelp() {
		commander.printOutln();
		commander.printOutln(helpUsage("rm_csd", " <LFN> [<LFN>...]"));
		commander.printOutln(helpStartOptions());
		commander.printOutln(helpOption("-r, -R", "remove directories and their contents recursively"));
		commander.printOutln();
	}

	/**
	 * rm cannot run without arguments
	 *
	 * @return <code>false</code>
	 */
	@Override
	public boolean canRunWithoutArguments() {
		return false;
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
	public JAliEnCommandrm_csd(final JAliEnCOMMander commander, final List<String> alArguments) throws OptionException {
		super(commander, alArguments);
		try {
			final OptionParser parser = new OptionParser();
			parser.accepts("r");
			parser.accepts("R");
			parser.accepts("v");

			final OptionSet options = parser.parse(alArguments.toArray(new String[] {}));

			bR = options.has("r") || options.has("R");
			bV = options.has("v");

			alPaths = optionToString(options.nonOptionArguments());
		}
		catch (final OptionException e) {
			commander.setReturnCode(ErrNo.EINVAL, e.getMessage());
			setArgumentsOk(false);
		}
	}
}
