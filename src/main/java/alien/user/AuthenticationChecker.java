package alien.user;

import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.Signature;
import java.security.SignatureException;
import java.security.cert.X509Certificate;
import java.security.interfaces.RSAPrivateKey;
import java.security.interfaces.RSAPublicKey;

import lazyj.Utils;

import org.bouncycastle.util.encoders.Hex;

import alien.config.ConfigUtils;

/**
 * @author ron
 * @since Jun 17, 2011
 */
public class AuthenticationChecker {

	private static RSAPrivateKey privKey = null;
	private static RSAPublicKey pubKey = null;
	private static X509Certificate cert = null;

	private String challenge = null;

	static {
		privKey = null; // (RSAPrivateKey) JAKeyStore.ks.getKey("User.cert",
						// JAKeyStore.pass);

		// Certificate[] usercert = null; //
		// JAKeyStore.ks.getCertificateChain("User.cert");
		pubKey = null; // (RSAPublicKey) usercert[0].getPublicKey();
	}

	/**
	 * @param pFinder
	 * @throws IOException
	 *             TODO : re-implement
	 */
	/*
	 * public static void loadPrivKey(final PasswordFinder pFinder) throws IOException {
	 * 
	 * if (privKey == null) { BufferedReader priv = null;
	 * 
	 * PEMParser reader = null;
	 * 
	 * try { priv = new BufferedReader(new FileReader(ConfigUtils.getConfig().gets("user.cert.priv.location").trim()));
	 * 
	 * if (pFinder.getPassword().length == 0) reader = new PEMParser(priv); else reader = new PEMParser(priv, pFinder);
	 * 
	 * privKey = (RSAPrivateKey) ((KeyPair) reader.readObject()).getPrivate(); } finally { try { if (reader != null) reader.close(); } catch (final IOException ioe) { // ignore }
	 * 
	 * try { if (priv != null) priv.close(); } catch (final IOException ioe) { // ignore } } } }
	 */

	/**
	 * @param pubCert
	 */
	public static void loadPubCert(final String pubCert) {
		final X509Certificate[] certChain = JAKeyStore.loadPubX509(pubCert, true);

		if (certChain != null) {
			cert = certChain[0];
			pubKey = (RSAPublicKey) cert.getPublicKey();
		}
		else
			System.err.println("Didn't find any certificate");
	}

	/**
	 * Read public user certificate from config location
	 * 
	 * @return the public certificate
	 * @throws IOException
	 */
	public static String readPubCert() throws IOException {
		final String location = ConfigUtils.getConfig().gets("user.cert.pub.location").trim();

		return Utils.readFile(location);
	}

	/**
	 * Create a challenge
	 * 
	 * @return the challenge
	 */
	public String challenge() {
		challenge = Long.toString(System.currentTimeMillis());
		return challenge;
	}

	/**
	 * sign the challenge as a response
	 * 
	 * @param challengeText
	 * @return the response
	 * @throws SignatureException
	 * @throws NoSuchAlgorithmException
	 * @throws InvalidKeyException
	 */
	public static String response(final String challengeText) throws SignatureException, NoSuchAlgorithmException, InvalidKeyException {
		final Signature signature = Signature.getInstance("SHA384withRSA");
		signature.initSign(privKey);
		signature.update(challengeText.getBytes());
		final byte[] signatureBytes = signature.sign();
		return new String(Hex.encode(signatureBytes));
	}

	/**
	 * verify the response as signature
	 * 
	 * @param pubCertString
	 * @param signature
	 * @return verified or not
	 * @throws SignatureException
	 * @throws NoSuchAlgorithmException
	 * @throws InvalidKeyException
	 */
	public boolean verify(final String pubCertString, final String signature) throws SignatureException, NoSuchAlgorithmException, InvalidKeyException {

		loadPubCert(pubCertString);
		final Signature verifier = Signature.getInstance("SHA384withRSA");
		verifier.initVerify(pubKey);
		verifier.update(challenge.getBytes());
		if (verifier.verify(Hex.decode(signature))) {
			System.out.println("Access granted for user: " + cert.getSubjectDN());
			return true;
		}

		System.out.println("Access denied");
		return false;
	}
}
