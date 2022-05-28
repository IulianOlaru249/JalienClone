package alien.shell.commands;

import java.util.Arrays;
import java.util.List;

import alien.catalogue.FileSystemUtils;
import alien.shell.ErrNo;
import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;

/**
 * @author mmmartin
 * @since November 27, 2018
 */

/**
 * chown
 */
public class JAliEnCommandchown_csd extends JAliEnBaseCommand {
	private String user;
	private String group;
	private String file;
	private boolean recursive;

	@Override
	public void run() {
		if (this.user == null || this.file == null) {
			commander.setReturnCode(ErrNo.EINVAL, "No user or file entered");
			return;
		}

		// boolean result = false;
		final String path = FileSystemUtils.getAbsolutePath(commander.user.getName(), commander.getCurrentDirName(), this.file);
		// run chown command
		final boolean result = commander.c_api.chownLFNCSD(path, this.user, this.group, this.recursive);

		if (!result)
			commander.setReturnCode(ErrNo.EIO, "Failed to chown file(s): " + path + (recursive ? "(recursive)" : "(non-recursive)"));
	}

	@Override
	public void printHelp() {
		commander.printOutln();
		commander.printOutln("Usage: chown_csd -R <user>[.<group>] <file>");
		commander.printOutln();
		commander.printOutln("Changes an owner or a group for a file");
		commander.printOutln("-R : do a recursive chown");
		commander.printOutln();
	}

	@Override
	public boolean canRunWithoutArguments() {
		return false;
	}

	/**
	 * @param commander
	 * @param alArguments
	 * @throws OptionException
	 */
	public JAliEnCommandchown_csd(final JAliEnCOMMander commander, final List<String> alArguments) throws OptionException {
		super(commander, alArguments);
		try {
			final OptionParser parser = new OptionParser();
			parser.accepts("R");

			final OptionSet options = parser.parse(alArguments.toArray(new String[] {}));

			final List<String> params = optionToString(options.nonOptionArguments());

			this.recursive = options.has("R");

			// check 2 arguments
			if (params.size() != 2)
				return;

			// get user/group
			final String[] usergrp = params.get(0).split("\\.");
			System.out.println(Arrays.toString(usergrp));
			this.user = usergrp[0];
			if (usergrp.length == 2)
				this.group = usergrp[1];

			// get file
			this.file = params.get(1);
		}
		catch (final OptionException e) {
			commander.setReturnCode(ErrNo.EINVAL, e.getMessage());
			setArgumentsOk(false);
		}
	}

}
