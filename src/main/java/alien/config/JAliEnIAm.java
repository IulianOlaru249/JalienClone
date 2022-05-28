package alien.config;

import java.util.Date;

import lazyj.Format;

/**
 * @author ron
 *
 */
public class JAliEnIAm {

	private static final String thathsMe = "jsh";

	private static final String myVersion = "v" + Version.getTag() + " (" + Format.showNamedDate(new Date(Version.getCompilationTimestamp())) + ")";

	private static final String myFullName = thathsMe + " " + myVersion;

	private static final String myVO = "alice";

	private static final String jshprompt = ":[" + myVO + "]";

	private static final String Iam = "JAliEn";

	private static final String IamPlain = "jalien";

	private static final String IamFullName = Iam + " " + myVersion;

	/**
	 * @return the name for the shell prompt
	 */
	public static String myJShPrompt() {
		return jshprompt;
	}

	/**
	 * @return my name
	 */
	public static String whatsMyName() {
		return thathsMe;
	}

	/**
	 * @return me and the version
	 */
	public static String whatsMyFullName() {
		return myFullName;
	}

	/**
	 * @return my version
	 */
	public static String whatsVersion() {
		return myVersion;
	}

	/**
	 * @return my version
	 */
	public static String whoamI() {
		return Iam;
	}

	/**
	 * @return my version
	 */
	public static String whoamIPlain() {
		return IamPlain;
	}

	/**
	 * @return my version
	 */
	public static String whoamIFullName() {
		return IamFullName;
	}

}
