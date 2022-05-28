package alien.taskQueue;

import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.Security;
import java.security.Signature;
import java.security.SignatureException;
import java.security.UnrecoverableKeyException;
import java.security.cert.Certificate;
import java.util.logging.Logger;

import java.security.cert.X509Certificate;

import org.bouncycastle.jce.provider.BouncyCastleProvider;

import alien.config.ConfigUtils;
import alien.io.xrootd.envelopes.Base64;
import alien.io.xrootd.envelopes.XrootDEnvelopeSigner;
import alien.user.AliEnPrincipal;
import alien.user.JAKeyStore;
import alien.user.UserFactory;

/**
 * @author ron
 * @since June 09, 2011
 */
public class JobSigner {

	/**
	 * logger
	 */
	static final Logger logger = ConfigUtils.getLogger(XrootDEnvelopeSigner.class.getCanonicalName());

	/**
	 * load the RSA keys for envelope signature, keys are supposed to be in pem, and can be created with: openssl req -x509 -nodes -days 365 -newkey rsa:4096 -keyout lpriv.pem -out lpub.pem
	 */
	static {
		Security.addProvider(new BouncyCastleProvider());

	}

	private static final String sJDLDelimOn = "<SJDL>\n";
	private static final String sJDLDelimOff = "</SJDL>\n";

	private static final String signatureDelimOn = "<SJDL_SIGNTATURE>";
	private static final String signatureDelimOff = "</SJDL_SIGNTATURE>\n";

	private static final String issuedDelimOn = "<SJDL_ISSUED>";
	private static final String issuedDelimOff = "</SJDL_ISSUED>\n";

	private static final String expiresDelimOn = "<SJDL_EXPIRES>";
	private static final String expiresDelimOff = "</SJDL_EXPIRES>\n";

	private static final String JDLDelimOn = "<Nested_JDL>\n";
	private static final String JDLDelimOff = "</Nested_JDL>\n";

	/**
	 * @param ks
	 * @param keyAlias
	 * @param pass
	 * @param alienUsername
	 * @param ojdl
	 * @throws NoSuchAlgorithmException
	 * @throws InvalidKeyException
	 * @throws SignatureException
	 * @return the signature of the jdl
	 */
	public static JDL signJob(final KeyStore ks, final String keyAlias, final char[] pass, @SuppressWarnings("unused") final String alienUsername, final JDL ojdl)
			throws NoSuchAlgorithmException, InvalidKeyException, SignatureException {

		if (ojdl == null)
			return null;

		final long issued = System.currentTimeMillis() / 1000L;
		String jdl = issuedDelimOn + issued + issuedDelimOff;
		jdl += expiresDelimOn + (issued + 60 * 60 * 24 * 14) + expiresDelimOff;

		jdl += JDLDelimOn + ojdl.toString() + JDLDelimOff;

		final Signature signer = Signature.getInstance("SHA384withRSA");

		try {
			signer.initSign((PrivateKey) ks.getKey(keyAlias, pass));
		}
		catch (final UnrecoverableKeyException e) {

			e.printStackTrace();
		}
		catch (final KeyStoreException e) {
			e.printStackTrace();
		}

		signer.update(jdl.getBytes());

		jdl = sJDLDelimOn + jdl;

		jdl += signatureDelimOn + Base64.encode(signer.sign()) + signatureDelimOff + sJDLDelimOff;

		// TODO : add signature to the JDL

		return ojdl;
	}

	/**
	 * @param ks
	 * @param keyAlias
	 * @param pass
	 * @param user
	 * @param origjdl
	 * @return <code>true</code> if the signature verifies
	 * @throws NoSuchAlgorithmException
	 * @throws InvalidKeyException
	 * @throws SignatureException
	 * @throws KeyStoreException
	 */
	@SuppressWarnings("unused")
	public static boolean verifyJob(final KeyStore ks, final String keyAlias, final char[] pass, final AliEnPrincipal user, final String origjdl)
			throws NoSuchAlgorithmException, InvalidKeyException, SignatureException, KeyStoreException {

		return false; // verifyJob(ks.getCertificate(keyAlias), user, origjdl);
	}

	/**
	 * @param cert
	 * @param sjdl
	 * @return <code>true</code> if the signature verifies
	 * @throws NoSuchAlgorithmException
	 * @throws InvalidKeyException
	 * @throws SignatureException
	 * @throws KeyStoreException
	 * @throws JobSubmissionException
	 */
	public static boolean verifyJobToRun(final X509Certificate[] cert, final String sjdl)
			throws NoSuchAlgorithmException, InvalidKeyException, SignatureException, KeyStoreException, JobSubmissionException {

		final Certificate[] ts = JAKeyStore.getKeyStore().getCertificateChain("User.cert");
		final X509Certificate[] tts = new X509Certificate[ts.length];
		for (int a = 0; a < ts.length; a++)
			tts[a] = (java.security.cert.X509Certificate) ts[a];

		System.out.println("Verifying central service signature...");
		if (verifyJob(tts, null, sjdl)) {
			final String nestedjdl = sjdl.substring(sjdl.indexOf(sJDLDelimOn) + sJDLDelimOn.length(), sjdl.lastIndexOf(signatureDelimOn));
			System.out.println("Verifying user signature...");
			return verifyJob(cert, UserFactory.getByCertificate(cert), nestedjdl);
		}
		return false;

	}

	/**
	 * @param cert
	 * @param user
	 * @param sjdl
	 * @return <code>true</code> if the signature verifies
	 * @throws NoSuchAlgorithmException
	 * @throws InvalidKeyException
	 * @throws SignatureException
	 * @throws KeyStoreException
	 * @throws JobSubmissionException
	 */
	public static boolean verifyJob(final X509Certificate[] cert, final AliEnPrincipal user, final String sjdl)
			throws NoSuchAlgorithmException, InvalidKeyException, SignatureException, KeyStoreException, JobSubmissionException {

		try {
			// System.out.println("we are verifying as JDL...:" + sjdl);
			// if(user!=null)
			// System.out.println("verifying user:" + user.getName());

			final String jdl = sjdl.substring(sjdl.indexOf(sJDLDelimOn) + sJDLDelimOn.length(), sjdl.lastIndexOf(signatureDelimOn));

			// System.out.println("jdl:|" + jdl + "|");

			final String signature = sjdl.substring(sjdl.lastIndexOf(signatureDelimOn) + signatureDelimOn.length(), sjdl.lastIndexOf(signatureDelimOff));

			// System.out.println("signature:|" + signature + "|");

			if (user != null) {
				JDL j;
				try {
					j = new JDL(sjdl);
					// System.out.println("user would be: " + j.getUser());
					if (!user.canBecome(j.getUser()))
						throw new JobSubmissionException(user.getName() + " cannot become " + j.getUser());
					// System.out.println("user authorized.");

				}
				catch (final IOException e) {
					throw new JobSubmissionException("Error while validating the JDL syntax", e);
				}
			}

			final long now = System.currentTimeMillis() / 1000L;

			long issued = 0;
			try {
				issued = Long.parseLong(sjdl.substring(sjdl.indexOf(issuedDelimOn) + issuedDelimOn.length(), sjdl.indexOf(issuedDelimOff)));
			}
			catch (final NumberFormatException e) {
				throw new JobSubmissionException("Invalid JDL Signature: [illegal issued tag]", e);
			}
			if (now < issued)
				throw new JobSubmissionException("Invalid JDL Signature: [not valid yet]");

			long expires = 0;
			try {
				expires = Long.parseLong(sjdl.substring(sjdl.indexOf(expiresDelimOn) + expiresDelimOn.length(), sjdl.indexOf(expiresDelimOff)));
			}
			catch (final NumberFormatException e) {
				throw new JobSubmissionException("Invalid JDL Signature: [illegal expires tag]", e);
			}
			if (now > expires)
				throw new JobSubmissionException("Invalid JDL Signature, [expired]");

			for (final X509Certificate c : cert) {
				final Signature verifyer = Signature.getInstance("SHA384withRSA");

				verifyer.initVerify(c.getPublicKey());

				verifyer.update(jdl.getBytes());

				if (verifyer.verify(Base64.decode(signature))) {
					System.out.println("Job signature verified:" + c.getSubjectDN());
					return true;
				}
				// System.out.println("This wasn't it:" + c.getSubjectDN());

			}

		}
		catch (final Throwable e) {
			e.printStackTrace();
			throw new JobSubmissionException("Invalid JDL Signature, [not verifyable: Exception]: " + e.getMessage());
		}
		throw new JobSubmissionException("Invalid JDL Signature, [not verifyable]");
	}

	@SuppressWarnings("unused")
	private static String parseJDLToSignString(final JDL jdl) {

		if (jdl.getExecutable().length() > 0) {
			final StringBuilder sjdl = new StringBuilder("Executable={\"").append(jdl.getExecutable()).append("\"};\n");

			sjdl.append("Arguments={");
			if (jdl.getArguments().size() > 0) {
				boolean first = true;

				for (final String arg : jdl.getArguments()) {
					if (!first)
						sjdl.append(',');
					else
						first = false;

					sjdl.append("\"").append(arg).append("\"");
				}
			}

			sjdl.append("};\n");

			if (!jdl.getUser().isEmpty())
				sjdl.append("User={\"").append(jdl.getUser()).append("\"};\n");

			sjdl.append("Output={");
			if (jdl.getOutputFiles().size() > 0) {
				boolean first = true;

				for (final String arg : jdl.getOutputFiles()) {
					if (!first)
						sjdl.append(',');
					else
						first = false;

					sjdl.append("\"").append(arg).append("\"");
				}
			}

			sjdl.append("};\n");

			sjdl.append("hashOrd=Executeable-Arguments-Output;\n");

			System.out.println("parsed JDL:" + sjdl);

			return sjdl.toString();
		}

		System.out.println("error parsing JDL!");

		return "";

	}
}
