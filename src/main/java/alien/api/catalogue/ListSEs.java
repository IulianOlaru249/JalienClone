package alien.api.catalogue;

import java.util.List;

import alien.api.Request;
import alien.se.SE;
import alien.se.SEUtils;
import alien.user.AliEnPrincipal;

/**
 * List the SEs with the given names (or all if the list is empty)
 *
 * @author costing
 * @since 2018-08-16
 */
public class ListSEs extends Request {
	/**
	 *
	 */
	private static final long serialVersionUID = 4476411896853649872L;
	private final List<String> requestedSEs;
	private List<SE> ses;

	/**
	 * @param user
	 * @param requestedSEs
	 */
	public ListSEs(final AliEnPrincipal user, final List<String> requestedSEs) {
		setRequestUser(user);
		this.requestedSEs = requestedSEs;
	}

	@Override
	public List<String> getArguments() {
		return requestedSEs;
	}

	/**
	 * @return SE list sorted by distance
	 */
	public List<SE> getSEs() {
		return this.ses;
	}

	@Override
	public void run() {
		if (requestedSEs != null && requestedSEs.size() == 0)
			ses = SEUtils.getSEs(null);
		else
			ses = SEUtils.getSEs(requestedSEs);
	}
}
