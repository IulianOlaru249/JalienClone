package utils.crawler;

import utils.StatusCode;

/**
 * The crawling status for each PFN analysed
 *
 * @author anegru
 */
public class CrawlingStatus {

	/**
	 * Crawling status code
	 */
	private final StatusCode code;

	/**
	 * Default error message
	 */
	private final String message;

	/**
	 * @param code
	 * @param message
	 */
	public CrawlingStatus(final StatusCode code, final String message) {
		this.code = code;
		this.message = message;
	}

	/**
	 * @return the status code associated to this constant
	 */
	public StatusCode getCode() {
		return code;
	}

	/**
	 * @return message for this constant
	 */
	public String getMessage() {
		return message;
	}

	@Override
	public String toString() {
		return "CrawlingStatus{" + "code=" + code + ", message='" + message + '\'' + '}';
	}
}