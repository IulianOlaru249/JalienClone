package alien.api.taskQueue;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import alien.api.Request;
import alien.taskQueue.JobBroker;
import alien.user.AliEnPrincipal;

/**
 * Get the number of waiting jobs fitting site requirements
 *
 * @author mmmartin
 * @since March 1, 2017
 */
public class GetNumberWaitingJobs extends Request {
	/**
	 *
	 */
	private static final long serialVersionUID = 5445861912342537975L;

	private HashMap<String, Object> match;
	private final HashMap<String, Object> matchRequest;

	/**
	 * @param user
	 * @param siteMap
	 */
	public GetNumberWaitingJobs(final AliEnPrincipal user, final HashMap<String, Object> siteMap) {
		setRequestUser(user);
		this.matchRequest = siteMap;
	}

	@Override
	public List<String> getArguments() {
		if (matchRequest != null)
			return Arrays.asList(matchRequest.toString());

		return null;
	}

	@Override
	public void run() {
		matchRequest.put("Remote", Integer.valueOf(0));
		this.match = JobBroker.getNumberWaitingForSite(matchRequest);

		if (((Integer) match.get("Code")).intValue() != 1) {
			matchRequest.put("Remote", Integer.valueOf(1));
			this.match = JobBroker.getNumberWaitingForSite(matchRequest);
		}
	}

	/**
	 * @return number of jobs
	 */
	public Integer getNumberJobsWaitingForSite() {
		if (((Integer) match.get("Code")).intValue() == 1)
			if (match.containsKey("counter")) {
				final String counter = (String) match.get("counter");
				if (counter == null || counter.equals(""))
					return Integer.valueOf(0);
				return Integer.valueOf(counter);
			}

		return Integer.valueOf(0);
	}

	@Override
	public String toString() {
		return "Asked for number of waiting jobs for site, reply is: " + this.match;
	}
}
