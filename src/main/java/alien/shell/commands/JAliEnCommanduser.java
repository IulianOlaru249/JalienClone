package alien.shell.commands;

import java.util.List;

import alien.shell.ErrNo;
import alien.user.AliEnPrincipal;
import alien.user.UserFactory;
import alien.user.UsersHelper;
import joptsimple.OptionException;

/**
 * @author ron
 * @since Oct 30, 2011
 */
public class JAliEnCommanduser extends JAliEnBaseCommand {

	private String user = null;

	@Override
	public void run() {
		final java.security.cert.X509Certificate[] cert = commander.user.getUserCert();
		AliEnPrincipal switchUser;
		final String defaultuser = commander.user.getDefaultUser();

		if (commander.user.canBecome(user)) {
			if ((switchUser = UserFactory.getByUsername(user)) != null)
				commander.user = switchUser;
			else if ((switchUser = UserFactory.getByRole(user)) != null)
				commander.user = switchUser;
			else {
				commander.setReturnCode(ErrNo.EINVAL, "User " + user + " cannot be found. Abort");
				return;
			}

			commander.user.setUserCert(cert);
			commander.user.setDefaultUser(defaultuser);

			final String userHomeDir = UsersHelper.getHomeDir(commander.user.getName());

			if (userHomeDir != null)
				commander.printOut("homedir", userHomeDir);
		}
		else {
			commander.setReturnCode(ErrNo.EPERM, "Switching user " + commander.user.getName() + " to [" + user + "] failed");
		}
	}

	/**
	 * printout the help info, none for this command
	 */
	@Override
	public void printHelp() {
		commander.printOutln();
		commander.printOutln(helpUsage("user", "<user name>"));
		commander.printOutln();
		commander.printOutln(helpParameter("Change effective role as specified."));
	}

	/**
	 * role can not run without arguments
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
	public JAliEnCommanduser(final JAliEnCOMMander commander, final List<String> alArguments) throws OptionException {
		super(commander, alArguments);

		if (alArguments.size() == 1)
			user = alArguments.get(0);
		else
			setArgumentsOk(false);
	}
}
