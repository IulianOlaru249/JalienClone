package alien.test;

import java.io.File;

import alien.config.JAliEnIAm;
import alien.test.setup.CreateCertificates;
import alien.test.setup.CreateDB;
import alien.test.setup.CreateLDAP;
import alien.test.setup.ManageSysEntities;
import alien.test.utils.TestCommand;
import alien.test.utils.TestException;
import alien.test.utils.TestService;

/**
 * @author ron
 * @since Sep 09, 2011
 */
public class SetupTestVO {

	/**
	 * @return <code>true</code> if successful
	 * @throws Exception
	 */
	protected static boolean setupVO() throws Exception {

		// step 0
		// --------------------------------------------------------------
		final File oldLink = new File(TestConfig.tvo_home);
		if (oldLink.exists()) {
			final TestCommand link = new TestCommand(new String[] { "rm", oldLink.getAbsolutePath() });
			if (!link.exec()) {
				final File target = new File(TestConfig.tvo_home + "_movedBy" + TestConfig.now);

				if (!oldLink.renameTo(target))
					throw new TestException("Cannot rename/move " + oldLink + " to " + target);
			}
			if (oldLink.exists())
				throw new TestException("Could not handle the old testVO entry: " + oldLink.getAbsolutePath());
		}

		TestConfig.initialize();

		System.out.println();
		System.out.println();
		System.out.println("Now let's start the work...");
		System.out.println("Creating TestVO in: " + TestConfig.tvo_real_home);

		if (!(new File(TestConfig.tvo_real_home)).mkdirs())
			throw new TestException("Could not create test VO directory.");
		final TestCommand link = new TestCommand(new String[] { "ln", "-s", TestConfig.tvo_real_home, TestConfig.tvo_home });
		if (!link.exec())
			throw new TestException("VO Setup ok, but final link setting failed.");
		// --------------------------------------------------------------
		//

		final boolean verbose = false;

		// step 1
		System.out.println("----- STEP1 [INIT]: Config -----");
		TestConfig.createConfig();
		System.out.println("----- STEP1 [DONE]: Config -----");

		// step 2
		System.out.println("----- STEP2 [INIT]: Certificates -----");
		if (!CreateCertificates.doit(verbose))
			throw new TestException("Creating Certificates failed.");
		System.out.println("----- STEP2 [DONE]: Certificates -----");

		// step 3
		System.out.println("----- STEP3 [INIT]: LDAP -----");
		if (!CreateLDAP.rampUpLDAP())
			throw new TestException("Initializing LDAP failed.");
		System.out.println("----- STEP3 [DONE]: LDAP -----");

		// step 4
		System.out.println("----- STEP4 [INIT]: DB -----");
		if (!CreateDB.rampUpDB())
			throw new TestException("Initializing DB failed.");
		System.out.println("----- STEP4 [DONE]: DB -----");

		// step 4
		System.out.println("----- STEP5 [INIT]: init VO -----");
		if (!rampUpVO())
			throw new TestException("Initializing VO failed.");
		System.out.println("----- STEP5 [DONE]: init VO -----");

		return true;
	}

	private static boolean rampUpVO() {

		boolean ret = true;

		if (!ManageSysEntities.addUser("admin", "1", "admin", "/C=CH/O=" + JAliEnIAm.whoamI() + "/CN=NOadmin"))
			ret = false;
		if (!ManageSysEntities.addUser(TestConfig.testUser, "2", "admin", TestConfig.certSubjectuser))
			ret = false;
		if (!ManageSysEntities.addSite(TestConfig.testSite, TestConfig.domain, "/tmp", "/tmp", "/tmp"))
			ret = false;
		if (!ManageSysEntities.addSE("firstse", "1", TestConfig.testSite, TestConfig.full_host_name + ":8092", "disk"))
			ret = false;

		return ret;

	}

	/**
	 * @return jCentral started successfully
	 * @throws Exception
	 */
	public static boolean startJCentral() throws Exception {

		final TestService jcentral = new TestService("jcentral");

		jcentral.start();

		Thread.sleep(2000);

		jcentral.interrupt();

		if (jcentral.getStatus() != 0)
			throw new TestException("The Thread of jCentral had an exception.");

		jcentral.interrupt();

		return true;
	}

	/**
	 * @return jBox started successfully
	 * @throws Exception
	 */
	public static boolean startJBox() throws Exception {

		final TestService jbox = new TestService("jbox");

		jbox.start();

		Thread.sleep(2000);

		jbox.interrupt();

		if (jbox.getStatus() != 0)
			throw new TestException("The Thread of jBox had an exception.");

		jbox.interrupt();

		return true;
	}

	/**
	 * @return jBox started successfully
	 * @throws Exception
	 */
	public static boolean startJShTests() throws Exception {

		final TestService jsh = new TestService("jsh");

		jsh.start();

		jsh.join();

		if (jsh.getStatus() != 0)
			throw new TestException("The Thread of jSh had an exception.");

		jsh.interrupt();

		return true;
	}

}
