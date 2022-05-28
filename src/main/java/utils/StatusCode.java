package utils;

/**
 * @author anegru
 */
public interface StatusCode {

	/**
	 * @return a detailed description of the status code
	 */
	String getDescription();

	/**
	 * @return a category for the status code. It can be regarded as a super class
	 */
	StatusType getType();
}
