package alien.shell.commands;

/**
 * @author ron
 * @since Oct 29, 2011
 */
public class JAliEnCommandException extends IllegalArgumentException {

	/**
	 *
	 */
	private static final long serialVersionUID = -2175875716474305315L;

	/**
	 *
	 */
	public JAliEnCommandException() {
		super();
	}

	/**
	 * @param message
	 */
	public JAliEnCommandException(final String message) {
		super(message);
	}

	/**
	 * @param message
	 * @param cause
	 */
	public JAliEnCommandException(final String message, final Throwable cause) {
		super(message, cause);
	}
}
