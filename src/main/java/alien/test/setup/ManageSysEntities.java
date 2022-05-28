package alien.test.setup;

import java.io.File;
import java.sql.SQLException;

import javax.naming.NamingException;

import alien.test.TestConfig;
import alien.test.utils.TestException;

/**
 * @author ron
 * @since October 8, 2011
 */
public class ManageSysEntities {

	/**
	 * 
	 * @param username
	 * @param uid
	 * @param role
	 * @param certSubject
	 * @return status of the user add
	 */
	public static boolean addUser(final String username, final String uid,
			final String role, final String certSubject) {

		try {
			CreateLDAP.addUserToLDAP(username, uid, role, certSubject);
			// CreateLDAP.addRoleToLDAP(username, username);
			CreateLDAP.addRoleToLDAP(role, username);

		}
		catch (Exception e) {
			System.out.println("LDAP Exception...");
			e.printStackTrace();
			return false;
		}

		try {

			CreateDB.addUserToDB(username, uid);

		}
		catch (SQLException e) {
			System.out.println("DB Syntax Exception: ");
			e.printStackTrace();
			return false;
		}
		catch (Exception e) {
			System.out.println("DB Exception...");
			e.printStackTrace();
			return false;
		}

		return true;
	}

	/**
	 * @param sitename
	 * @param domain
	 * @param logdir
	 * @param cachedir
	 * @param tmpdir
	 * @return status of the site add
	 */
	public static boolean addSite(final String sitename, final String domain,
			final String logdir, final String cachedir, final String tmpdir) {

		try {
			CreateLDAP
					.addSiteToLDAP(sitename, domain, logdir, cachedir, tmpdir);
		}
		catch (NamingException ne) {
			ne.printStackTrace();
			return false;
		}
		return true;

	}

	/**
	 * @param seName
	 * @param seNumber
	 * @param site
	 * @param iodeamon
	 * @param qos
	 * @return status of the SE add
	 */
	public static boolean addSE(final String seName, final String seNumber, final String site,
			final String iodeamon, final String qos) {

		try {
			final String freespace = "2000000000";
			final String storedir = TestConfig.se_home + "/" + seName;

			CreateLDAP
					.addSEToLDAP(seName, site, iodeamon, storedir, qos);

			File se = new File(storedir);
			if (!se.mkdir())
				throw new TestException("Could not create SE directory: "
						+ storedir);

			CreateDB.addSEtoDB(seName, seNumber, site, iodeamon, storedir, qos, freespace);
		}
		catch (Exception e) {
			e.printStackTrace();
			return false;
		}
		return true;
	}

}
