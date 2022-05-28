package alien.io.xrootd.envelopes;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.LineNumberReader;
import java.security.GeneralSecurityException;
import java.security.KeyFactory;
import java.security.KeyPair;
import java.security.NoSuchAlgorithmException;
import java.security.interfaces.RSAPrivateKey;
import java.security.interfaces.RSAPublicKey;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.X509EncodedKeySpec;
import java.util.Hashtable;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.StringTokenizer;

/**
 * @author Steffen
 * @since Nov 9, 2010
 */
public class TokenAuthorizationFactory {
	private Map<String, KeyPair> keystore;
	private File keystoreFile;

	/**
	 * Dirty hack: will be deprecated soon
	 */
	private String _noStrongAuthz;

	/**
	 * @return handler
	 */
	public TokenAuthzHandler getAuthzHandler() {
		return new TokenAuthzHandler(_noStrongAuthz, keystore);
	}

	private void loadKeyStore() throws Exception, IOException {
		try (LineNumberReader in = new LineNumberReader(new FileReader(keystoreFile))) {
			// reset keystore
			keystore = new Hashtable<>();

			// the RSA keyfactory
			KeyFactory keyFactory = null;

			try {
				// initialise RSA key factory
				keyFactory = KeyFactory.getInstance("RSA");
			}
			catch (final NoSuchAlgorithmException e) {
				throw new RuntimeException("Failed to initialize RSA key factory" + e.getMessage());
			}

			String line = null;
			while ((line = in.readLine()) != null) {
				final StringTokenizer tokenizer = new StringTokenizer(line, " \t");

				String voToken = null;
				String privKeyToken = null;
				String pubKeyToken = null;

				try {

					// ignore comment lines and any lines not starting
					// with the keyword 'KEY'
					final String firstToken = tokenizer.nextToken();
					if (firstToken.startsWith("#") || !firstToken.equals("KEY"))
						continue;

					voToken = tokenizer.nextToken();
					privKeyToken = tokenizer.nextToken();
					pubKeyToken = tokenizer.nextToken();

				}
				catch (final NoSuchElementException e) {
					throw new Exception("line no " + (in.getLineNumber()) + " : invalid format", e);
				}

				if (!(voToken.startsWith("VO:") && privKeyToken.startsWith("PRIVKEY:") && pubKeyToken.startsWith("PUBKEY:")))
					throw new Exception("line no " + (in.getLineNumber()) + " : invalid format");

				keystore.put(voToken.substring(voToken.indexOf(':') + 1),
						loadKeyPair(privKeyToken.substring(privKeyToken.indexOf(':') + 1), pubKeyToken.substring(pubKeyToken.indexOf(':') + 1), keyFactory));
			}
		}
	}

	private static KeyPair loadKeyPair(final String privKeyFileName, final String pubKeyFileName, final KeyFactory keyFactory) throws IOException {
		final File privKeyFile = new File(privKeyFileName);
		final File pubKeyFile = new File(pubKeyFileName);

		final byte[] privKeyArray = readKeyfile(privKeyFile);
		// logger.debug("read private keyfile "+privKeyFile+" ("+privKeyArray.length+" bytes)");
		// store private key (DER-encoded) in PKCS8-representation object
		final PKCS8EncodedKeySpec privKeySpec = new PKCS8EncodedKeySpec(privKeyArray);
		// parse unencrypted private key into java private key object
		RSAPrivateKey privKey;
		try {
			privKey = (RSAPrivateKey) keyFactory.generatePrivate(privKeySpec);
		}
		catch (final InvalidKeySpecException e) {
			throw new IOException("error loading private key " + privKeyFileName + ": " + e.getMessage());
		}

		final byte[] pubKeyArray = readKeyfile(pubKeyFile);
		// logger.debug("Read public keyfile "+pubKeyFile+" ("+pubKeyArray.length+" bytes)");
		// store the public key (DER-encodedn ot PEM) into a X.509 certificate
		// object
		final X509EncodedKeySpec pubKeySpec = new X509EncodedKeySpec(pubKeyArray);
		RSAPublicKey pubKey;
		try {
			pubKey = (RSAPublicKey) keyFactory.generatePublic(pubKeySpec);
		}
		catch (final InvalidKeySpecException e) {
			throw new IOException("error loading public key " + pubKeyFileName + ": " + e.getMessage());
		}

		return new KeyPair(pubKey, privKey);
	}

	/**
	 * Helper method thats reads a file.
	 *
	 * @param file
	 *            the File which is going to be read
	 * @return an array which holds the file content
	 * @throws IOException
	 *             if reading the file fails
	 */
	private static byte[] readKeyfile(final File file) throws IOException {
		try (InputStream in = new FileInputStream(file)) {
			final byte[] result = new byte[(int) file.length()];
			int bytesRead = 0;

			while ((bytesRead += in.read(result, bytesRead, (int) file.length() - bytesRead)) < file.length()) {
				// nothing
			}

			if (bytesRead != file.length())
				throw new IOException("Keyfile " + file.getName() + " corrupt.");

			return result;
		}
	}

	/**
	 * @throws GeneralSecurityException
	 */
	public void init() throws GeneralSecurityException {
		try {
			loadKeyStore();
		}
		catch (final Exception e) {
			throw new GeneralSecurityException("unable to load keystore: " + e.getMessage());
		}
	}

	/**
	 * @param file
	 */
	public void setKeystore(final String file) {
		keystoreFile = new File(file);
	}

	/**
	 * @return keystore file
	 */
	public String getKeystore() {
		return keystoreFile.toString();
	}

	/**
	 * @param auth
	 */
	public void setNoStrongAuthorization(final String auth) {
		_noStrongAuthz = auth;
	}

	/**
	 * @return ?
	 */
	public String getNoStrongAuthorization() {
		return _noStrongAuthz;
	}
}
