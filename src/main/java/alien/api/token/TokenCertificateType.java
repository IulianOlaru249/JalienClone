package alien.api.token;

/**
 * Possible token certificate types
 *
 * @author costing
 * @since 2017-07-04
 */
public enum TokenCertificateType {
	/**
	 * The request is for a user token certificate
	 */
	USER_CERTIFICATE(31),
	/**
	 * The request is for a job token
	 */
	JOB_TOKEN(2),
	/**
	 * A generic Job Agent identity that will be able to then request an actual job and run it
	 */
	JOB_AGENT_TOKEN(2),
	/**
	 * Trusted host (VoBox)
	 */
	HOST(365);

	private int maxValidityDays;

	private TokenCertificateType(final int maxValidityDays) {
		this.maxValidityDays = maxValidityDays;
	}

	/**
	 * Get the max validity (in days) of the certificates that can be generated for each type
	 *
	 * @return max validity (in days)
	 */
	public int getMaxValidity() {
		return maxValidityDays;
	}
}
