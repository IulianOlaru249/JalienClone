package alien;

import java.io.File;
import java.io.IOException;

import alien.site.Functions;

/**
 * @author ron
 *
 */
public class TestBuggy {

	/**
	 * @param args
	 */
	public static void main(final String[] args) {

		try {
			Functions.unzip(new File("testsys/ldap_schema.zip"), new File("/tmp/"));
		}
		catch (final IOException e) {
			e.printStackTrace();
			System.err.println("error unzipping ldap schema");
		}
	}
}
