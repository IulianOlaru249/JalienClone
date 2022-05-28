package alien.test;

import java.util.Calendar;
import java.util.logging.Logger;

import alien.catalogue.CatalogueUtils;
import alien.config.ConfigUtils;
import alien.test.chapters.TestCentralUtils;
import alien.test.chapters.TestJShOverJBox;
import alien.test.setup.CreateDB;
import alien.test.setup.CreateLDAP;
import alien.test.utils.TestException;

/**
 * @author ron
 * @since October 09, 2011
 */
public class Tester {

	/**
	 * Logger
	 */
	static final Logger logger = ConfigUtils.getLogger(CatalogueUtils.class.getCanonicalName());

	// static{
	// ConfigUtils.getVersion();
	// }

	/**
	 * @param args
	 * @throws Exception
	 */
	public static void main(final String[] args) throws Exception {

		System.out.println();
		System.out.println();
		System.out.println();
		System.out.println("---- jAliEn Test System ----");

		if (!TestBrain.findCommands()) {
			interrupt("Necessary programs missing.");
			return;
		}

		cleanUp();
		System.out.println("Don't worry about any errors up to here!");
		System.out.println();
		System.out.println("Starting ...");

		try {
			System.out.println("Life is a race, so we take the time here.");
			final long start = Calendar.getInstance().getTimeInMillis();

			System.out.println();
			System.out.println("############ Chapter 0, VO SETUP      ########");

			if (SetupTestVO.setupVO())
				System.out.println("Test VO ready and running after " + giveMeATiming(start) + "s .");
			else
				throw new TestException("VO Setup failed. No exceptions.");

			System.out.println();
			System.out.println("############ Chapter 1, Central Tests ########");

			if (TestCentralUtils.runTestChapter())
				System.out.println("Central Tests successful.");
			else
				throw new TestException("Central Tests failed. No exceptions.");

			System.out.println();
			System.out.println("############ Chapter 2, jCentral_2_jBox Tests     ########");
			if (SetupTestVO.startJCentral())
				System.out.println("jCentral started successful.");
			else
				throw new TestException("jCentral failed to start. No exceptions.");

			System.out.println();

			if (SetupTestVO.startJBox())
				System.out.println("jBox started successful.");
			else
				throw new TestException("jBox failed to start. No exceptions.");
			System.out.println();
			System.out.println();

			// if (!SetupTestVO.startJShTests())
			// throw new
			// TestException("JSh Tests failed to start. No exceptions.");

			TestJShOverJBox.runTestChapter();

			System.out.println("Finished  TestCentralUtils after " + giveMeATiming(start) + "s .");

		}
		catch (final TestException e) {
			interrupt(e.getMessage());
			return;
		}

	}

	private static void interrupt(final String message) {
		System.err.println("We have a fatal problem: " + message);
		System.err.println("Shutting down...");
		System.err.println();
		System.err.println("May the force be with u next time!");
	}

	private static long giveMeATiming(final long start) {

		return (Calendar.getInstance().getTimeInMillis() - start) / 1000;

	}

	private static void cleanUp() {
		try{
			CreateDB.stopDatabase();
		}
		catch(@SuppressWarnings("unused") final Exception e){
			//ignore
		}
		try{
			CreateLDAP.stopLDAP();
		}
		catch(@SuppressWarnings("unused") final Exception e){
			//ignore
		}
	}
}
