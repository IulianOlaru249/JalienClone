package alien.api.catalogue;

import java.util.ArrayList;
import java.util.Collection;
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
 * @author costing
 * @since Mar 09, 2021
 */
public class GUIDfromUUID extends Request {
	/**
	 *
	 */
	private static final long serialVersionUID = -5599554036424707834L;

	private final Collection<UUID> sguids;
	private final boolean resolveLFNs;

	private Collection<GUID> guids;

	/**
	 * @param user
	 * @param sguids
	 * @param resolveLFNs
	 */
	public GUIDfromUUID(final AliEnPrincipal user, final Collection<UUID> sguids, final boolean resolveLFNs) {
		setRequestUser(user);
		this.sguids = sguids;
		this.resolveLFNs = resolveLFNs;
	}

	@Override
	public List<String> getArguments() {
		final ArrayList<String> arguments = new ArrayList<>(2);

		arguments.add(String.valueOf(resolveLFNs));
		arguments.add(sguids.toString());

		return arguments;
	}

	@Override
	public void run() {
		this.guids = GUIDUtils.getGUIDs(sguids.toArray(new UUID[0]));

		if (resolveLFNs) {
			final List<LFN> lfns = LFNUtils.getLFNs(guids);

			if (lfns != null)
				for (final LFN l : lfns)
					for (final GUID g : this.guids)
						if (g.guid.equals(l.guid)) {
							g.addKnownLFN(l);
							break;
						}
		}
	}

	/**
	 * @return the requested GUID
	 */
	public Collection<GUID> getGUIDs() {
		return this.guids;
	}

	@Override
	public String toString() {
		return "Asked for : " + this.sguids + ", reply is " + this.guids.size() + " long";
	}
}
