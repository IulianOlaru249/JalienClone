package utils.crawler;

import static utils.StatusType.FILE_CORRUPT;
import static utils.StatusType.FILE_INACCESSIBLE;
import static utils.StatusType.FILE_OK;
import static utils.StatusType.INTERNAL_ERROR;
import static utils.StatusType.UNEXPECTED_ERROR;

import utils.StatusCode;
import utils.StatusType;

/**
 * The crawling status for each PFN analysed
 *
 * @author anegru
 */
public enum CrawlingStatusCode implements StatusCode {

	/**
	 * The checksum of the downloaded file matches the checksum registered in the catalogue
	 */
	S_FILE_CHECKSUM_MATCH("The checksum of the downloaded file matches the checksum registered in the catalogue", FILE_OK),

	/**
	 * The checksum of the downloaded file does not match the checksum registered in the catalogue
	 */
	S_FILE_CHECKSUM_MISMATCH("The checksum of the downloaded file does not match the checksum registered in the catalogue", FILE_CORRUPT),

	/**
	 * The size of the downloaded file is 0 bytes
	 */
	E_FILE_EMPTY("The size of the downloaded file is 0 bytes", FILE_CORRUPT),

	/**
	 * The PFN access token cannot be filled
	 */
	E_PFN_NOT_READABLE("The PFN access token cannot be filled", FILE_INACCESSIBLE),

	/**
	 * Xrdstat returned that the PFN is offline
	 */
	E_PFN_OFFLINE("Xrdstat returned that the PFN is offline", FILE_INACCESSIBLE),

	/**
	 * The file download process failed
	 */
	E_PFN_DOWNLOAD_FAILED("The file download process failed", INTERNAL_ERROR),

	/**
	 * The xrdstat call for the PFN failed
	 */
	E_PFN_XRDSTAT_FAILED("The xrdstat call for the PFN failed", INTERNAL_ERROR),

	/**
	 * The MD5 registered in the catalogue is null
	 */
	E_CATALOGUE_MD5_IS_NULL("The MD5 registered in the catalogue is null", FILE_OK),

	/**
	 * The MD5 registered in the catalogue is blank
	 */
	E_CATALOGUE_MD5_IS_BLANK("The MD5 registered in the catalogue is blank", FILE_OK),

	/**
	 * The GUID cannot be retrieved from the PFN
	 */
	E_GUID_NOT_FOUND("The GUID cannot be retrieved from the PFN", INTERNAL_ERROR),

	/**
	 * The crawling process results in an unexpected error. None of the above cases matches the error.
	 */
	E_UNEXPECTED_ERROR("The crawling process results in an unexpected error. None of the above cases matches the error.", UNEXPECTED_ERROR);

	/**
	 * Status description
	 */
	private final String description;

	/**
	 * Status type
	 */
	private final StatusType type;

	CrawlingStatusCode(final String description, final StatusType type) {
		this.description = description;
		this.type = type;
	}

	/**
	 * @return the description of the crawling status code
	 */
	@Override
	public String getDescription() {
		return description;
	}

	/**
	 * @return the type of the crawling status
	 */
	@Override
	public StatusType getType() {
		return type;
	}
}