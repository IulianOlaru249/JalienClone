package alien.io.protocols;

import java.io.IOException;

/**
 * @author costing
 *
 */
public class TargetException extends IOException {

	/**
	 *
	 */
	private static final long serialVersionUID = -5073609846381630091L;

	/**
	 * @param reason
	 */
	public TargetException(final String reason) {
		super(reason);
	}

	/**
	 * @param reason
	 * @param cause
	 */
	public TargetException(final String reason, final Throwable cause) {
		super(reason, cause);
	}
}
