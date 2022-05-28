package utils;

/**
 * @author anegru
 */
public enum StatusType {
	/**
	 * All checks were ok
	 */
	FILE_OK,
	/**
	 * Content could be retrieved but the checksums don't match
	 */
	FILE_CORRUPT,
	/**
	 * File cannot be retrieved
	 */
	FILE_INACCESSIBLE,
	/**
	 * Code problems / exceptions etc
	 */
	INTERNAL_ERROR,
	/**
	 * Everything else
	 */
	UNEXPECTED_ERROR,
}
