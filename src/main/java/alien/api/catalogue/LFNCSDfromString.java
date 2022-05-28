package alien.api.catalogue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.UUID;

import alien.api.Request;
import alien.catalogue.LFNCSDUtils;
import alien.catalogue.LFN_CSD;
import alien.user.AliEnPrincipal;

/**
 * Get the LFNCSD object for this path
 *
 * @author mmmartin
 * @since November 23, 2018
 */
public class LFNCSDfromString extends Request {

	/**
	 *
	 */
	private static final long serialVersionUID = -1720547988105993481L;

	private final Collection<String> path;
	private final boolean ignoreFolders;
	private final boolean evenIfDoesntExist;
	private final boolean lfns_are_uuids;

	private List<LFN_CSD> lfns = null;

	/**
	 * @param user
	 * @param evenIfDoesntExist
	 * @param ignoreFolders
	 * @param lfns_are_uuids
	 * @param paths
	 */
	public LFNCSDfromString(final AliEnPrincipal user, final boolean evenIfDoesntExist, final boolean ignoreFolders, final boolean lfns_are_uuids, final Collection<String> paths) {
		setRequestUser(user);
		this.path = paths;
		this.ignoreFolders = ignoreFolders;
		this.evenIfDoesntExist = evenIfDoesntExist;
		this.lfns_are_uuids = lfns_are_uuids;
	}

	@Override
	public List<String> getArguments() {
		return Arrays.asList(this.path != null ? this.path.toString() : null, String.valueOf(this.ignoreFolders), String.valueOf(this.evenIfDoesntExist), String.valueOf(lfns_are_uuids));
	}

	@Override
	public void run() {
		if (evenIfDoesntExist)
			for (final String s : path) {
				LFN_CSD l = null;
				if (lfns_are_uuids)
					l = LFNCSDUtils.guid2lfn(UUID.fromString(s));
				else
					l = LFNCSDUtils.getLFN(s, evenIfDoesntExist);

				if (l != null && !(l.isDirectory() && ignoreFolders)) {
					if (this.lfns == null)
						this.lfns = new ArrayList<>();

					this.lfns.add(l);
				}
			}
		// else
		// this.lfns = LFNCSDUtils.getLFNs(ignoreFolders, path);
	}

	/**
	 * @return the requested LFNCSD
	 */
	public List<LFN_CSD> getLFNs() {
		return this.lfns;
	}

	@Override
	public String toString() {
		return "Asked for : " + this.path + " (" + this.ignoreFolders + "), reply is: " + this.lfns;
	}
}
