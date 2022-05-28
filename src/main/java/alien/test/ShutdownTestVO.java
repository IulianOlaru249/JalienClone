package alien.test;

import alien.test.setup.CreateDB;
import alien.test.setup.CreateLDAP;

/**
 * @author ron
 * @since Sep 09, 2011
 */
public class ShutdownTestVO {

	/**
	 * @param args
	 * @throws Exception
	 */
	public static void main(final String[] args) throws Exception {

		if (!TestBrain.findCommands()) {
			System.err.println("Necessary programs missing.");
			return;
		}

		CreateLDAP.stopLDAP();

		CreateDB.stopDatabase();

	}
}
