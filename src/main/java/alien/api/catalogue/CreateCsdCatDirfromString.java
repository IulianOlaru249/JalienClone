package alien.api.catalogue;

import java.util.Arrays;
import java.util.List;

import alien.api.Request;
import alien.catalogue.LFNCSDUtils;
import alien.catalogue.LFN_CSD;
import alien.user.AliEnPrincipal;

/**
 *
 * @author mmmartin
 * @since November 23, 2018
 */
public class CreateCsdCatDirfromString extends Request {

	/**
	 *
	 */
	private static final long serialVersionUID = -8266256263179660234L;
	private final String path;
	private boolean createNonExistentParents = false;

	private LFN_CSD lfn;

	/**
	 * @param user
	 * @param path
	 * @param createNonExistentParents
	 */
	public CreateCsdCatDirfromString(final AliEnPrincipal user, final String path, final boolean createNonExistentParents) {
		setRequestUser(user);
		this.path = path;
		this.createNonExistentParents = createNonExistentParents;
	}

	@Override
	public List<String> getArguments() {
		return Arrays.asList(this.path, String.valueOf(this.createNonExistentParents));
	}

	@Override
	public void run() {
		this.lfn = LFNCSDUtils.mkdir(getEffectiveRequester(), path, createNonExistentParents);
	}

	/**
	 * @return the created LFNCSD of the directory
	 */
	public LFN_CSD getDir() {
		return this.lfn;
	}

	@Override
	public String toString() {
		return "Asked for : " + this.path + ", reply is:\n" + this.lfn;
	}
}
