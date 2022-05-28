package alien.api.catalogue;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import alien.api.Cacheable;
import alien.api.Request;
import alien.catalogue.GUID;
import alien.catalogue.GUIDUtils;
import alien.catalogue.PFN;
import alien.user.AliEnPrincipal;

/**
 *
 * @author ron
 * @since Jun 03, 2011
 */
public class PFNfromString extends Request implements Cacheable {

	private static final long serialVersionUID = -3237006644358177225L;

	private final String sguid;

	private Set<PFN> pfns;

	/**
	 * Get PFNs by String
	 *
	 * @param user
	 * @param sguid
	 */
	public PFNfromString(final AliEnPrincipal user, final String sguid) {
		setRequestUser(user);
		this.sguid = sguid;
	}

	@Override
	public List<String> getArguments() {
		return Arrays.asList(this.sguid);
	}

	@Override
	public void run() {
		final UUID u = UUID.fromString(sguid);

		final GUID g = GUIDUtils.getGUID(u);

		if (g != null)
			this.pfns = g.getPFNs();
		else
			this.pfns = null;
	}

	/**
	 * @return the requested PFNs
	 */
	public Set<PFN> getPFNs() {
		return this.pfns;
	}

	@Override
	public String toString() {
		return "Asked for : " + this.sguid + " , reply is:\n" + this.pfns;
	}

	@Override
	public String getKey() {
		return this.sguid;
	}

	@Override
	public long getTimeout() {
		return 1000 * 15;
	}
}
