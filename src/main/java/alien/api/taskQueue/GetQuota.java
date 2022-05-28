package alien.api.taskQueue;

import java.util.Arrays;
import java.util.List;

import alien.api.Request;
import alien.quotas.Quota;
import alien.quotas.QuotaUtilities;
import alien.user.AliEnPrincipal;

/**
 * Get job quota for a given user
 */
public class GetQuota extends Request {
	/**
	 *
	 */
	private static final long serialVersionUID = 7852648478812622364L;
	private final String username;
	private Quota q;

	/**
	 * @param user
	 */
	public GetQuota(final AliEnPrincipal user) {
		this.username = user.getName();
	}

	@Override
	public List<String> getArguments() {
		return Arrays.asList(username);
	}

	@Override
	public void run() {
		this.q = QuotaUtilities.getJobQuota(this.username);
	}

	/**
	 * @return job quota
	 */
	public Quota getQuota() {
		return this.q;
	}
}
