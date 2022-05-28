package alien.io.protocols;

import java.io.IOException;

/**
 * @author costing
 */
public class SourceException extends IOException {

	/**
	 *
	 */
	private static final long serialVersionUID = -5073609846381630091L;

	/**
	 * Status code of the exception
	 */
	private final SourceExceptionCode code;

	/**
	 * @param code error code
	 * @param reason
	 */
	public SourceException(final SourceExceptionCode code, final String reason) {
		super(reason);
		this.code = code;
	}

	/**
	 * @param code error code
	 * @param reason
	 * @param cause
	 */
	public SourceException(final SourceExceptionCode code, final String reason, final Throwable cause) {
		super(reason, cause);
		this.code = code;
	}

	/**
	 * @return the status code associated to this exception
	 */
	public SourceExceptionCode getCode() {
		return code;
	}
}
