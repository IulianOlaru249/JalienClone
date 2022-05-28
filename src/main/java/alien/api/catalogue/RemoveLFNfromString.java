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
public class RemoveLFNfromString extends Request {

	/**
	 *
	 */
	private static final long serialVersionUID = 8507879864667855615L;
	private final String path;

	private boolean wasRemoved = false;
	private boolean recursive = false;
	private boolean purge = true;

	/**
	 * @param user
	 * @param path
	 * @param recursive
	 */
	public RemoveLFNfromString(final AliEnPrincipal user, final String path, final boolean recursive) {
		setRequestUser(user);
		this.path = path;
		this.recursive = recursive;
		this.purge = true;
	}

	@Override
	public List<String> getArguments() {
		return Arrays.asList(path, String.valueOf(recursive), String.valueOf(purge));
	}

	/**
	 * @param user
	 * @param path
	 * @param recursive
	 * @param purge
	 */
	public RemoveLFNfromString(final AliEnPrincipal user, final String path, final boolean recursive, final boolean purge) {
		setRequestUser(user);
		this.path = path;
		this.recursive = recursive;
		this.purge = purge;
	}

	@Override
	public void run() {
		final LFN lfn = LFNUtils.getLFN(path);
		if (lfn != null)
			wasRemoved = LFNUtils.rmLFN(getEffectiveRequester(), lfn, recursive, purge);

	}

	/**
	 * @return the status of the LFN's removal
	 */
	public boolean wasRemoved() {
		return this.wasRemoved;
	}

	@Override
	public String toString() {
		return "Asked to remove : " + this.path + ", reply is:\n" + this.wasRemoved;
	}
}
