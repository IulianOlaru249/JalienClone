package alien.user;

import java.util.Set;

import alien.config.ConfigUtils;

/**
 * @author costing
 * @since 2010-06-29
 */
public class UsersHelper {

	/**
	 * Get some user's home directory in AliEn
	 * 
	 * @param sUsername
	 *            account name
	 * @return home directory, ending with "/"
	 */
	public static String getHomeDir(final String sUsername) {
		if (sUsername == null || sUsername.length() == 0)
			return null;

		final Set<String> homeDir = LDAPHelper.checkLdapInformation("uid=" + sUsername, "ou=People,", "homeDirectory");

		if (homeDir != null && homeDir.size() > 0) {
			String sDir = homeDir.iterator().next();

			if (sDir != null && sDir.length() > 0) {
				if (!sDir.endsWith("/"))
					sDir = sDir + "/";

				return sDir;
			}
		}

		return getDefaultUserDir(sUsername);
	}

	private static String usersBaseHomeDir = null;

	/**
	 * Get the base AliEn directory where all user accounts are created
	 * 
	 * @return base directory, ending with "/"
	 */
	public static String getUsersBaseHomeDir() {
		if (usersBaseHomeDir == null) {
			usersBaseHomeDir = ConfigUtils.getConfig().gets("alien.users.basehomedir", "/alice/cern.ch/user/");

			if (!usersBaseHomeDir.endsWith("/"))
				usersBaseHomeDir += "/";
		}

		return usersBaseHomeDir;
	}

	/**
	 * Get the default home dir of an account (might not be the one in ldap)
	 * 
	 * @param sAccount
	 *            account name
	 * @return default home dir, ending with "/", or <code>null</code> if the
	 *         account is not valid
	 */
	public static String getDefaultUserDir(final String sAccount) {
		if (sAccount == null || sAccount.length() == 0)
			return null;

		return getUsersBaseHomeDir() + sAccount.charAt(0) + "/" + sAccount + "/";
	}

}
