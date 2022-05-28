package alien.shell.commands;

import java.util.List;

import alien.catalogue.FileSystemUtils;
import alien.catalogue.LFN_CSD;
import alien.shell.ErrNo;
import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;

/**
 * @author mmmartin
 * @since November 26, 2018
 */
public class JAliEnCommandmv_csd extends JAliEnBaseCommand {

	private String[] sources = null;

	private String target = null;

	/**
	 * Size of the argument list.
	 */
	int size = 0;

	@Override
	public void run() {
		if (size == 0 || size == 1) {
			printHelp();
			return;
		}

		final String fullTarget = FileSystemUtils.getAbsolutePath(commander.user.getName(), commander.getCurrentDirName(), target);

		// if we have several sources, the destination must be a folder
		if (size > 2) {
			final LFN_CSD lfnc_target = commander.c_api.getLFNCSD(fullTarget);

			if (lfnc_target == null || !lfnc_target.isDirectory()) {
				commander.setReturnCode(ErrNo.ENOTDIR, "When moving several sources, the destination must be a directory");
				return;
			}
		}

		// mv each source to target
		for (int i = 0; i <= size - 2; i++) {
			final String fullSource = FileSystemUtils.getAbsolutePath(commander.user.getName(), commander.getCurrentDirName(), sources[i]);

			commander.printOutln("mv " + fullSource + " -> " + fullTarget);

			final int code = commander.c_api.moveLFNCSD(fullSource, fullTarget);

			switch (code) {
				case 1:
					commander.setReturnCode(ErrNo.EINVAL, "Source and destination are the same");
					break;
				case 2:
					commander.setReturnCode(ErrNo.ENOENT, "The destination parent doesn't exist");
					break;
				case 3:
					commander.setReturnCode(ErrNo.EPERM, "No permission to write on destination");
					break;
				case 4:
					commander.setReturnCode(ErrNo.EIO, "Some entries failed to be moved");
					break;
				case 5:
					commander.setReturnCode(ErrNo.EPERM, "No permission to move the source");
					break;
				case 6:
					commander.setReturnCode(ErrNo.EIO, "Cannot mv " + fullSource + "into " + fullTarget);
					break;
				default:
					break;
			}

		}
	}

	/**
	 * printout the help info, none for this command
	 */
	@Override
	public void printHelp() {
		commander.printOutln();
		commander.printOutln(helpUsage("mv_csd", " <LFN1> [<LFN2>...]  <targetLFN>"));
		commander.printOutln();
	}

	/**
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
	 */
	public JAliEnCommandmv_csd(final JAliEnCOMMander commander, final List<String> alArguments) {
		super(commander, alArguments);

		try {
			final OptionParser parser = new OptionParser();

			final OptionSet options = parser.parse(alArguments.toArray(new String[] {}));

			final List<String> nonOptionArguments = optionToString(options.nonOptionArguments());

			size = nonOptionArguments.size();
			sources = new String[size - 1];
			for (int i = 0; i <= (size - 2); i++)
				sources[i] = nonOptionArguments.get(i);

			target = nonOptionArguments.get(size - 1);

		}
		catch (final OptionException e) {
			commander.setReturnCode(ErrNo.EINVAL, e.getMessage());
			setArgumentsOk(false);
		}
	}
}
