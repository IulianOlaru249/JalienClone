package alien.api.catalogue;

import java.util.Arrays;
import java.util.List;

import alien.api.Request;
import alien.catalogue.LFNCSDUtils;
import alien.catalogue.LFN_CSD;
import alien.user.AliEnPrincipal;

/**
 * touch an LFNCSD
 */
public class TouchLFNCSDfromString extends Request {

	private static final long serialVersionUID = -2792425667105358661L;
	private final String path;

	private boolean success;

	private LFN_CSD lfnc;

	/**
	 * @param user
	 * @param path
	 */
	public TouchLFNCSDfromString(final AliEnPrincipal user, final String path) {
		setRequestUser(user);
		this.path = path;
	}

	@Override
	public List<String> getArguments() {
		return Arrays.asList(path);
	}

	@Override
	public void run() {
		this.lfnc = LFNCSDUtils.getLFN(path, true);
		this.success = LFNCSDUtils.touch(getEffectiveRequester(), lfnc);
	}

	/**
	 * @return the created LFNCSD of the directory
	 */
	public LFN_CSD getLFN() {
		return this.lfnc;
	}

	/**
	 * @return <code>true</code> if the LFNCSD was touched
	 */
	public boolean isSuccessful() {
		return this.success;
	}

	@Override
	public String toString() {
		return "Asked for : " + this.path + ", reply is:\n" + this.lfnc;
	}
}
