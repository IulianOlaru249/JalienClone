package alien.shell.commands;

import java.util.List;

import alien.catalogue.FileSystemUtils;
import alien.shell.ErrNo;

/**
 *
 */
public class JAliEnCommandtouch extends JAliEnBaseCommand {
	private final List<String> filelist;

	@Override
	public void run() {
		for (final String path : this.filelist) {
			if (commander.c_api.touchLFN(FileSystemUtils.getAbsolutePath(commander.user.getName(), commander.getCurrentDirName(), path)) == null) {
				commander.setReturnCode(ErrNo.EREMOTEIO, "Failed to touch the LFN: " + FileSystemUtils.getAbsolutePath(commander.user.getName(), commander.getCurrentDirName(), path));
			}
		}
	}

	@Override
	public void printHelp() {
		commander.printOutln();
		commander.printOutln(helpUsage("touch", " <LFN> [<LFN>[,<LFN>]]"));
		commander.printOutln();
	}

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
	public JAliEnCommandtouch(final JAliEnCOMMander commander, final List<String> alArguments) {
		super(commander, alArguments);

		filelist = alArguments;
	}
}
