package alien;

import java.io.File;
import java.io.IOException;
import java.security.GeneralSecurityException;

import alien.io.IOUtils;
import alien.user.UserFactory;

/**
 * Testing stuff
 *
 * @author costing
 *
 */
public class Testing {

	/**
	 * @param args
	 * @throws IOException
	 * @throws GeneralSecurityException
	 */
	public static void main(final String[] args) throws IOException, GeneralSecurityException {
		IOUtils.upload(new File("/etc/passwd"), "testIOUpload", UserFactory.getByUsername("grigoras"), 10, System.err, false);
	}

}
