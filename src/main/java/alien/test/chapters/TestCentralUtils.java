package alien.test.chapters;

import alien.catalogue.LFN;
import alien.catalogue.LFNUtils;
import alien.test.TestConfig;
import alien.test.setup.CreateLDAP;
import alien.user.AliEnPrincipal;
import alien.user.AuthorizationChecker;
import alien.user.UserFactory;

/**
 * @author ron
 * @since October 09, 2011
 */
public class TestCentralUtils {

	private static int tno = 0;

	private static int cno = 0;

	private static boolean finalStatus = true;

	/**
	 * @return status
	 */
	public static boolean runTestChapter() {

		tno = 14;
		cno = 1;

		System.out.println();
		System.out.println("-----------------  Testing LDAP plain access -----------------");

		test("get user [" + TestConfig.testUser + "]", possibleToGetUserOverLDAP(TestConfig.testUser));

		AliEnPrincipal user = UserFactory.getByUsername(TestConfig.testUser);

		System.out.println();
		System.out.println("-----------------  Testing DB plain access   -----------------");
		test("get /  ", possibleToGetCatalogueEntry("/"));

		test("get " + TestConfig.base_home_dir, possibleToGetCatalogueEntry(TestConfig.base_home_dir));

		test("get " + TestConfig.base_home_dir + TestConfig.testUser.substring(0, 1) + "/", possibleToGetCatalogueEntry(TestConfig.base_home_dir + TestConfig.testUser.substring(0, 1) + "/"));

		test("get " + CreateLDAP.getUserHome(TestConfig.testUser), possibleToGetCatalogueEntry(CreateLDAP.getUserHome(TestConfig.testUser)));

		System.out.println();
		System.out.println("-----------------    Testing user access     -----------------");

		test("access-read / ", possibleToAccessEntry(user, LFNUtils.getLFN("/"), true, false));

		test("access-write / ", possibleToAccessEntry(user, LFNUtils.getLFN("/"), true, false));

		test("access-read " + TestConfig.base_home_dir, possibleToAccessEntry(user, LFNUtils.getLFN(TestConfig.base_home_dir), false, true));

		test("access-write " + TestConfig.base_home_dir, possibleToAccessEntry(user, LFNUtils.getLFN(TestConfig.base_home_dir), true, false));

		test("access-read " + TestConfig.base_home_dir + TestConfig.testUser.substring(0, 1) + "/",
				possibleToAccessEntry(user, LFNUtils.getLFN(TestConfig.base_home_dir + TestConfig.testUser.substring(0, 1) + "/"), false, true));

		test("access-write " + TestConfig.base_home_dir + TestConfig.testUser.substring(0, 1) + "/",
				possibleToAccessEntry(user, LFNUtils.getLFN(TestConfig.base_home_dir + TestConfig.testUser.substring(0, 1) + "/"), true, false));

		test("access-read " + CreateLDAP.getUserHome(TestConfig.testUser),
				possibleToAccessEntry(user, LFNUtils.getLFN(CreateLDAP.getUserHome(TestConfig.testUser)), false, true));

		test("access-write " + CreateLDAP.getUserHome(TestConfig.testUser),
				possibleToAccessEntry(user, LFNUtils.getLFN(CreateLDAP.getUserHome(TestConfig.testUser)), true, true));

		test("createDir " + CreateLDAP.getUserHome(TestConfig.testUser) + "bin",
				possibleToCreateDir(user, CreateLDAP.getUserHome(TestConfig.testUser) + "bin"));

		System.out.println("--------------------------------------");

		return finalStatus;

	}

	private static boolean possibleToGetUserOverLDAP(String username) {

		return (username.equals(UserFactory.getByUsername(username).getName()));

	}

	private static boolean possibleToGetCatalogueEntry(String name) {
		LFN lfn = LFNUtils.getLFN(name);
		if (lfn != null)
			return true;
		return false;
	}

	private static boolean possibleToCreateDir(final AliEnPrincipal user, String dirname) {
		LFN l = LFNUtils.mkdir(user, dirname);
		if (l == null)
			return false;
		return true;
	}

	private static boolean possibleToAccessEntry(
			final AliEnPrincipal user, final LFN lfn, final boolean writeTest, final boolean weWanted) {

		if (writeTest)
			return (AuthorizationChecker.canWrite(lfn, user) == weWanted);

		return (AuthorizationChecker.canRead(lfn, user) == weWanted);
	}

	private static void test(final String desc, final boolean res) {
		System.out.print("----- jCentral UtilsTest " + cno + "/" + tno);
		if (res) {
			System.out.print(" [ok] ");
		}
		else {
			System.out.print(" {NO} !!!!!!");
			finalStatus = false;
		}
		System.out.println("    , which was: " + desc);
		cno++;
	}

}
