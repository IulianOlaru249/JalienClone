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
public class MoveLFNCSDfromString extends Request {

	/**
	 *
	 */
	private static final long serialVersionUID = 5206724705550117953L;

	private final String path;

	private final String newpath;

	private int code;

	/**
	 * @param user
	 * @param path
	 * @param newpath
	 */
	public MoveLFNCSDfromString(final AliEnPrincipal user, final String path, final String newpath) {
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
		code = LFNCSDUtils.mv(getEffectiveRequester(), path, newpath);
	}

	/**
	 * @return the code of the LFNCSD move
	 */
	public int getMvCode() {
		return this.code;
	}

	@Override
	public String toString() {
		return "Asked to mv : " + this.path + " to " + this.newpath + ", reply is:\n" + this.code;
	}
}
