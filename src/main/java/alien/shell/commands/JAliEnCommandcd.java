package alien.shell.commands;

import java.util.List;

import alien.catalogue.FileSystemUtils;
import alien.catalogue.LFN;
import alien.shell.ErrNo;
import alien.user.UsersHelper;

/**
 * @author ron
 * @since June 4, 2011
 * @author sraje (Shikhar Raje, IIIT Hyderabad)
 * @since Modified 27th July, 2012
 */
public class JAliEnCommandcd extends JAliEnBaseCommand {

	@Override
	public void run() {

		LFN newDir = null;

		if (alArguments != null && alArguments.size() > 0)
			newDir = commander.c_api.getLFN(FileSystemUtils.getAbsolutePath(commander.user.getName(), commander.getCurrentDirName(), alArguments.get(0)));
		else
			newDir = commander.c_api.getLFN(UsersHelper.getHomeDir(commander.user.getName()));

		if (newDir != null) {
			if (newDir.isDirectory()) {
				commander.curDir = newDir;
			}
			else
				commander.setReturnCode(ErrNo.ENOTDIR, alArguments.get(0));
		}
		else
			commander.setReturnCode(ErrNo.ENOENT);
	}

	/**
	 * printout the help info
	 */
	@Override
	public void printHelp() {
		commander.printOutln();
		commander.printOutln(helpUsage("cd", "[dir]"));
		commander.printOutln();
	}

	/**
	 * cd can run without arguments
	 *
	 * @return <code>true</code>
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
	 */
	public JAliEnCommandcd(final JAliEnCOMMander commander, final List<String> alArguments) {
		super(commander, alArguments);
	}
}
