package alien.api.catalogue;

import java.util.Arrays;
import java.util.List;

import alien.api.Request;
import alien.catalogue.LFN;
import alien.catalogue.LFNUtils;
import alien.user.AliEnPrincipal;

/**
 * touch an LFN
 */
public class TouchLFNfromString extends Request {

	private static final long serialVersionUID = -2792425667105358669L;
	private final String path;

	private boolean success;

	private LFN lfn;

	/**
	 * @param user
	 * @param path
	 */
	public TouchLFNfromString(final AliEnPrincipal user, final String path) {
		setRequestUser(user);
		this.path = path;
		// this.createNonExistentParents = createNonExistentParents;
	}

	@Override
	public List<String> getArguments() {
		return Arrays.asList(path);
	}

	@Override
	public void run() {
		// if(createNonExistentParents)
		this.lfn = LFNUtils.getLFN(path, true);
		this.success = LFNUtils.touchLFN(getEffectiveRequester(), this.lfn);
		// else
		// this.lfn = FileSystemUtils.createCatalogueDirectory(user, path);

	}

	/**
	 * @return the created LFN of the directory
	 */
	public LFN getLFN() {
		return this.lfn;
	}

	/**
	 * @return <code>true</code> if the LFN was touched
	 */
	public boolean isSuccessful() {
		return this.success;
	}

	@Override
	public String toString() {
		return "Asked for : " + this.path + ", reply is:\n" + this.lfn;
	}
}
