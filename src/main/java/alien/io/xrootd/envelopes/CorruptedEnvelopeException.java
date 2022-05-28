package alien.io.xrootd.envelopes;

/**
 * @author Steffen
 * @since Nov 9, 2010
 */
public class CorruptedEnvelopeException extends Exception {

	/**
	 *
	 */
	private static final long serialVersionUID = 1L;

	/**
	 * @param message
	 */
	public CorruptedEnvelopeException(final String message) {
		super(message);
		System.out.println(message);
	}

	@SuppressWarnings("javadoc")
	public CorruptedEnvelopeException(final String message, final Throwable cause) {
		super(message, cause);
	}
}
