package alien.api.catalogue;

import java.util.Arrays;
import java.util.List;

import alien.api.Request;
import alien.catalogue.LFN;
import alien.catalogue.LFNUtils;
import alien.user.AliEnPrincipal;

/**
 * Get the LFN object for this path
 *
 * @author ron
 * @since Jun 08, 2011
 */
public class LFNListingfromString extends Request {

	/**
	 *
	 */
	private static final long serialVersionUID = -7167353294190733455L;
	private final String path;

	private List<LFN> lfns = null;

	/**
	 * @param user
	 * @param path
	 */
	public LFNListingfromString(final AliEnPrincipal user, final String path) {
		setRequestUser(user);
		this.path = path;
	}

	@Override
	public List<String> getArguments() {
		return Arrays.asList(this.path);
	}

	@Override
	public void run() {
		final LFN entry = LFNUtils.getLFN(path, false);

		if (entry != null)
			if (entry.type == 'd')
				this.lfns = entry.list();
			else
				this.lfns = Arrays.asList(entry);
	}

	/**
	 * @return the requested LFN or the file listing for a directory or <code>null</code> if LFN doesn't exist
	 */
	public List<LFN> getLFNs() {
		return this.lfns;
	}

	@Override
	public String toString() {
		return "Asked for : " + this.path + " reply is: " + this.lfns;
	}
}
