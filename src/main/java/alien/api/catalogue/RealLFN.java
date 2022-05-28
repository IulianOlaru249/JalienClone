package alien.api.catalogue;

import java.util.Arrays;
import java.util.List;

import alien.api.Request;
import alien.catalogue.LFN;
import alien.catalogue.LFNUtils;
import alien.user.AliEnPrincipal;

/**
 *
 * @author costing
 * @since 2018-05-17
 */
public class RealLFN extends Request {

	private static final long serialVersionUID = 8631851052133487066L;

	private LFN lfn;
	private final String sLFN;

	private LFN realLFN = null;

	/**
	 * Resolve the real LFN of an existing LFN object
	 *
	 * @param user
	 *            user who makes the request
	 * @param lfn
	 *            requested archive member to resolve
	 */
	public RealLFN(final AliEnPrincipal user, final LFN lfn) {
		setRequestUser(user);
		this.lfn = lfn;
		this.sLFN = null;
	}

	@Override
	public List<String> getArguments() {
		return Arrays.asList(this.lfn != null ? this.lfn.getCanonicalName() : null);
	}

	/**
	 * Resolve the archive member and the real LFN of it
	 *
	 * @param user
	 *            user who makes the request
	 * @param sLFN
	 *            requested archive member to resolve (first to an LFN object and then to the real LFN)
	 */
	public RealLFN(final AliEnPrincipal user, final String sLFN) {
		setRequestUser(user);
		this.sLFN = sLFN;
		this.lfn = null;
	}

	@Override
	public void run() {
		if (this.lfn == null)
			this.lfn = LFNUtils.getLFN(sLFN);

		if (this.lfn == null)
			return;

		this.realLFN = LFNUtils.getRealLFN(lfn);
	}

	/**
	 * @return the archive member that was requested (or resolved from a string)
	 */
	public LFN getLFN() {
		return this.lfn;
	}

	/**
	 * @return the zip archive containing the given archive member (or the same file in case it's a physical file to begin with)
	 */
	public LFN getRealLFN() {
		return this.realLFN;
	}

	/**
	 * @return requested file name, if the request was started from a String
	 */
	public String getRequestedName() {
		return this.sLFN;
	}

}
