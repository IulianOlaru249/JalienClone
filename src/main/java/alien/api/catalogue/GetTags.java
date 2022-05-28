package alien.api.catalogue;

import java.util.Arrays;
import java.util.List;
import java.util.Set;

import alien.api.Request;
import alien.catalogue.LFNUtils;
import alien.user.AliEnPrincipal;

/**
 * Get the available tags associated to a path
 *
 * @author costing
 * @since 2020-02-25
 */
public class GetTags extends Request {
	private static final long serialVersionUID = -8097151852196189205L;

	private final String path;
	private Set<String> tags;

	/**
	 * @param user
	 * @param path
	 */
	public GetTags(final AliEnPrincipal user, final String path) {
		setRequestUser(user);
		this.path = path;
	}

	@Override
	public List<String> getArguments() {
		return Arrays.asList(this.path);
	}

	@Override
	public void run() {
		this.tags = LFNUtils.getTags(path);
	}

	/**
	 * @return the requested GUID
	 */
	public Set<String> getTags() {
		return this.tags;
	}

	@Override
	public String toString() {
		return "Asked for : " + this.path + ", reply is: " + this.tags;
	}
}
