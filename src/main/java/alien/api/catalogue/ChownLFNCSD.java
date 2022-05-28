package alien.api.catalogue;

import java.util.Arrays;
import java.util.List;

import alien.api.Request;
import alien.catalogue.LFNCSDUtils;
import alien.user.AliEnPrincipal;

/**
 * @author mmmartin
 * @since November 27, 2018
 */

/**
 * chown request
 */
public class ChownLFNCSD extends Request {
	/**
	 *
	 */
	private static final long serialVersionUID = -4209526023185462133L;
	private final String path;
	private final String chown_user;
	private final String chown_group;
	private boolean success;
	private final boolean recursive;
	// private HashMap<String, Boolean> results;

	/**
	 * @param user
	 * @param fpath
	 * @param chuser
	 * @param chgroup
	 * @param recursive
	 */
	public ChownLFNCSD(final AliEnPrincipal user, final String fpath, final String chuser, final String chgroup, final boolean recursive) {
		setRequestUser(user);
		path = fpath;
		chown_user = chuser;
		chown_group = chgroup;
		this.recursive = recursive;
	}

	@Override
	public List<String> getArguments() {
		return Arrays.asList(path, chown_user, chown_group, String.valueOf(this.recursive));
	}

	@Override
	public void run() {
		// results = new HashMap<>();
		success = LFNCSDUtils.chown(getEffectiveRequester(), path, chown_user, chown_group, recursive);
	}

	/**
	 * @return chown status
	 */
	public boolean getSuccess() {
		return success;
	}

	// /**
	// * @return operation status per lfn
	// */
	// public HashMap<String, Boolean> getResults() {
	// return results;
	// }

}
