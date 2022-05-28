package alien.api.catalogue;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import alien.api.Request;
import alien.catalogue.LFNCSDUtils;
import alien.catalogue.LFN_CSD;
import alien.user.AliEnPrincipal;

/**
 * Get the LFN object for this path
 *
 * @author ron
 * @since Jun 08, 2011
 */
public class LFNCSDListingfromString extends Request {

	/**
	 *
	 */
	private static final long serialVersionUID = -7167353294190733459L;
	private final String path;

	private Collection<LFN_CSD> lfns = null;

	/**
	 * @param user
	 * @param path
	 */
	public LFNCSDListingfromString(final AliEnPrincipal user, final String path) {
		setRequestUser(user);
		this.path = path;
	}

	@Override
	public List<String> getArguments() {
		return Arrays.asList(this.path);
	}

	@Override
	public void run() {
		this.lfns = LFNCSDUtils.ls(path);
	}

	/**
	 * @return the requested LFN
	 */
	public Collection<LFN_CSD> getLFNs() {
		return this.lfns;
	}

	@Override
	public String toString() {
		return "Asked for : " + this.path + " reply is: " + this.lfns;
	}
}
