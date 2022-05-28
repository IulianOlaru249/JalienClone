package alien.api.catalogue;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import alien.api.Request;
import alien.catalogue.GUID;
import alien.catalogue.GUIDUtils;
import alien.catalogue.LFN;
import alien.catalogue.LFNUtils;
import alien.user.AliEnPrincipal;

/**
 *
 * @author ron
 * @since Jun 03, 2011
 */
public class GUIDfromString extends Request {

	/**
	 *
	 */
	private static final long serialVersionUID = -3670065132137151044L;
	private final String sguid;
	private final boolean evenIfDoesNotExist;
	private final boolean resolveLFNs;

	private GUID guid;

	/**
	 * @param user
	 * @param sguid
	 * @param evenIfDoesNotExist
	 * @param resolveLFNs
	 */
	public GUIDfromString(final AliEnPrincipal user, final String sguid, final boolean evenIfDoesNotExist, final boolean resolveLFNs) {
		setRequestUser(user);
		this.sguid = sguid;
		this.evenIfDoesNotExist = evenIfDoesNotExist;
		this.resolveLFNs = resolveLFNs;
	}

	@Override
	public List<String> getArguments() {
		return Arrays.asList(this.sguid, String.valueOf(this.evenIfDoesNotExist), String.valueOf(this.resolveLFNs));
	}

	@Override
	public void run() {
		this.guid = GUIDUtils.getGUID(UUID.fromString(sguid), evenIfDoesNotExist);

		if (resolveLFNs)
			if (this.guid != null && this.guid.getLFNs() == null) {
				final LFN l = LFNUtils.getLFN(this.guid);

				if (l != null)
					this.guid.addKnownLFN(l);
			}
	}

	/**
	 * @return the requested GUID
	 */
	public GUID getGUID() {
		return this.guid;
	}

	@Override
	public String toString() {
		return "Asked for : " + this.sguid + " (" + this.evenIfDoesNotExist + "), reply is:\n" + this.guid;
	}
}
