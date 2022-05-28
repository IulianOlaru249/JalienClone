package alien.taskQueue;

/**
 * @author ron
 * @since Jun 21, 2011
 */
public class JobSubmissionException extends Exception {

	/**
	 *
	 */
	private static final long serialVersionUID = 2548219734472243569L;

	/**
	 * @param s
	 */
	public JobSubmissionException(final String s) {
		super(s);
	}

	/**
	 * @param message
	 * @param cause
	 */
	public JobSubmissionException(final String message, final Throwable cause) {
		super(message, cause);
	}
}
