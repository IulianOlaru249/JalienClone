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
 * @since Nov 21, 2011
 */
public class MoveLFNfromString extends Request {

	/**
	 *
	 */
	private static final long serialVersionUID = 5206724705550117952L;

	private final String path;

	private final String newpath;

	private LFN newLFN = null;

	/**
	 * @param user
	 * @param path
	 * @param newpath
	 */
	public MoveLFNfromString(final AliEnPrincipal user, final String path, final String newpath) {
		setRequestUser(user);
		this.path = path;
		this.newpath = newpath;
	}

	@Override
	public List<String> getArguments() {
		return Arrays.asList(this.path, this.newpath);
	}

	@Override
	public void run() {
		final LFN lfn = LFNUtils.getLFN(path);

		if (lfn != null)
			newLFN = LFNUtils.mvLFN(getEffectiveRequester(), lfn, newpath);

	}

	/**
	 * @return the status of the LFN's removal
	 */
	public LFN newLFN() {
		return this.newLFN;
	}

	@Override
	public String toString() {
		return "Asked to mv : " + this.path + " to " + this.newpath + ", reply is:\n" + this.newLFN;
	}
}
