package alien.api.catalogue;

import java.util.Arrays;
import java.util.List;

import alien.api.Request;
import alien.catalogue.LFNCSDUtils;
import alien.user.AliEnPrincipal;

/**
 *
 * @author mmmartin
 * @since November 27, 2018
 */
public class RemoveLFNCSDfromString extends Request {

	/**
	 *
	 */
	private static final long serialVersionUID = 8507879864667855616L;
	private final String path;

	private boolean wasRemoved = false;
	private boolean recursive = false;
	private boolean purge = true;

	/**
	 * @param user
	 * @param path
	 * @param recursive
	 */
	public RemoveLFNCSDfromString(final AliEnPrincipal user, final String path, final boolean recursive) {
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
	public RemoveLFNCSDfromString(final AliEnPrincipal user, final String path, final boolean recursive, final boolean purge) {
		setRequestUser(user);
		this.path = path;
		this.recursive = recursive;
		this.purge = purge;
	}

	@Override
	public void run() {
		wasRemoved = LFNCSDUtils.delete(getEffectiveRequester(), path, purge, recursive, true);
	}

	/**
	 * @return the status of the LFNCSD removal
	 */
	public boolean wasRemoved() {
		return this.wasRemoved;
	}

	@Override
	public String toString() {
		return "Asked to remove : " + this.path + ", reply is:\n" + this.wasRemoved;
	}
}
