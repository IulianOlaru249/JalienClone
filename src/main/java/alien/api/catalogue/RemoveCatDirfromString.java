package alien.api.catalogue;

import java.util.Arrays;
import java.util.List;

import alien.api.Request;
import alien.catalogue.LFN;
import alien.catalogue.LFNUtils;
import alien.user.AliEnPrincipal;

/**
 *
 * @author ron
 * @since Oct 27, 2011
 */
public class RemoveCatDirfromString extends Request {

	/**
	 *
	 */
	private static final long serialVersionUID = 5481660419374953794L;

	private final String path;

	private boolean wasRemoved = false;

	/**
	 * @param user
	 * @param path
	 */
	public RemoveCatDirfromString(final AliEnPrincipal user, final String path) {
		setRequestUser(user);
		this.path = path;
	}

	@Override
	public List<String> getArguments() {
		return Arrays.asList(path);
	}

	@Override
	public void run() {
		final LFN lfn = LFNUtils.getLFN(path);
		if (lfn != null)
			wasRemoved = LFNUtils.rmdir(getEffectiveRequester(), lfn, false);

	}

	/**
	 * @return the status of the directory's removal
	 */
	public boolean wasRemoved() {
		return this.wasRemoved;
	}

	@Override
	public String toString() {
		return "Asked to remove : " + this.path + ", reply is:\n" + this.wasRemoved;
	}
}
