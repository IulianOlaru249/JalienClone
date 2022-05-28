package alien.shell;

/**
 * @author ron
 * @since Oct 27, 2011
 */
public class ShellColor {

	private static final String prefix = "\033[";

	private static final String sep = ";";

	private static final String suffix = "m";

	/**
	 * @return color code
	 */
	public static String reset() {
		return genTag(Style.CLEAR);
	}

	/**
	 * @return color code
	 */
	public static String bold() {
		return genTag(Style.CLEAR);
	}

	/**
	 * @return color code for error Message
	 */
	public static String errorMessage() {
		return boldRed();
	}

	/**
	 * @return color code for info Message
	 */
	public static String infoMessage() {
		return boldBlack();
	}

	/**
	 * @return color code
	 */
	public static String boldRed() {
		return genTag(ForeColor.RED, Style.BOLD);
	}

	/**
	 * @return color code
	 */
	public static String boldWhite() {
		return genTag(ForeColor.WHITE, Style.BOLD);
	}

	/**
	 * @return color code
	 */
	public static String blue() {
		return genTag(ForeColor.BLUE);
	}

	/**
	 * @return color code
	 */
	public static String black() {
		return genTag(ForeColor.BLACK, Style.NONE);
	}

	/**
	 * @return color code
	 */
	public static String boldBlack() {
		return genTag(ForeColor.BLACK, Style.BOLD);
	}

	/**
	 * @return color code
	 */
	public static String jobStateRed() {
		return genTag(ForeColor.RED, BackColor.BLACK);
	}

	/**
	 * @return color code
	 */
	public static String jobStateRedError() {
		return genTag(ForeColor.RED, BackColor.NEUUNFVIERZIG);
	}

	/**
	 * @return color code
	 */
	public static String jobStateBlue() {
		return genTag(ForeColor.BLUE);
	}

	/**
	 * @return color code
	 */
	public static String jobStateBlueError() {
		return genTag(ForeColor.BLUE, BackColor.NEUUNFVIERZIG);
	}

	/**
	 * @return color code
	 */
	public static String jobStateGreen() {
		return genTag(ForeColor.GREEN);
	}

	/**
	 * @return color code
	 */
	public static String jobStateYellow() {
		return genTag(ForeColor.YELLOW);
	}

	private static String genTag(final ShellCol one) {
		return prefix + one.getCode() + suffix;
	}

	private static String genTag(final ShellCol one, final ShellCol two) {
		return prefix + one.getCode() + sep + two.getCode() + suffix;
	}

	// private static String genTag(final ShellCol one, final ShellCol two,
	// final Style three) {
	// return prefix + one.getCode() + sep + two.getCode() + sep +
	// three.getCode() + suffix;
	// }

	private enum ForeColor implements ShellCol {

		BLACK("30"), RED("31"), GREEN("32"), YELLOW("33"), BLUE("34"), MAGENTA("35"), CYAN("36"), WHITE("37"), NONE("");

		private final String code;

		ForeColor(final String code) {
			this.code = code;
		}

		@Override
		public String getCode() {
			return this.code;
		}

	}

	private enum BackColor implements ShellCol {

		BLACK("40"), RED("41"), GREEN("42"), YELLOW("43"), BLUE("44"), MAGENTA("45"), CYAN("46"), WHITE("47"), NEUUNFVIERZIG("49"), NONE("");

		private final String code;

		BackColor(final String code) {
			this.code = code;
		}

		@Override
		public String getCode() {
			return this.code;
		}

	}

	private enum Style implements ShellCol {

		CLEAR("0"), BOLD("1"), LIGHT("1"), DARK("2"), UNDERLINE("4"), REVERSE("7"), HIDDEN("8"), NONE("");

		private final String code;

		Style(final String code) {
			this.code = code;
		}

		@Override
		public String getCode() {
			return this.code;
		}

	}

	private interface ShellCol {
		abstract public String getCode();

	}

}
