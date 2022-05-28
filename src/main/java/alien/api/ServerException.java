package alien.api;

/**
 * @author costing
 *
 */
public class ServerException extends Exception {

	/**
	 * 
	 */
	private static final long serialVersionUID = 7409345562258484573L;

	/**
	 * A server exception that must be propagated back to the client
	 * 
	 * @param message
	 * @param cause underlying cause of the exception
	 */
	public ServerException(final String message, final Throwable cause) {
		super(message, cause);
	}

}
