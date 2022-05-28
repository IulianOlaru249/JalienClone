package alien.shell.commands;

import java.util.List;

import alien.catalogue.FileSystemUtils;
import alien.shell.ErrNo;
import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;

/**
 * @author mmmartin
 * @since November 23, 2018 <br>
 *        usage: mkdir [-options] directory [directory[,directory]] <br>
 *        options: <br>
 *        -p : create parents as needed <br>
 *        -silent : execute command silently <br>
 */
public class JAliEnCommandmkdir_csd extends JAliEnBaseCommand {

	/**
	 * marker for -p argument : create parents as needed
	 */
	private boolean bP = false;

	/**
	 * the list of directories that will be created
	 */
	private List<String> alPaths = null;

	@Override
	public void run() {

		for (final String path : alPaths) {
			if (commander.c_api.createCatalogueDirectoryCsd(FileSystemUtils.getAbsolutePath(commander.user.getName(), commander.getCurrentDirName(), path), bP) == null)
				commander.setReturnCode(ErrNo.ENOENT, "Could not create directory (or non-existing parents): " + path);
		}

		final JAliEnCommandmkdir md = new JAliEnCommandmkdir(commander, alPaths);
		md.run();
	}

	/**
	 * printout the help info
	 */
	@Override
	public void printHelp() {
		commander.printOutln();
		commander.printOutln(helpUsage("mkdir_csd", "[-options] <directory> [<directory>[,<directory>]]"));
		commander.printOutln(helpStartOptions());
		commander.printOutln(helpOption("-p", "create parents as needed"));
		commander.printOutln(helpOption("-silent", "execute command silently"));
		commander.printOutln();
	}

	/**
	 * mkdir cannot run without arguments
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
	public JAliEnCommandmkdir_csd(final JAliEnCOMMander commander, final List<String> alArguments) throws OptionException {
		super(commander, alArguments);

		try {
			final OptionParser parser = new OptionParser();

			parser.accepts("p");
			parser.accepts("s");

			final OptionSet options = parser.parse(alArguments.toArray(new String[] {}));

			alPaths = optionToString(options.nonOptionArguments());

			if (options.has("s"))
				silent();
			bP = options.has("p");
		}
		catch (final OptionException e) {
			commander.setReturnCode(ErrNo.EINVAL, e.getMessage());
			setArgumentsOk(false);
		}
	}
}
