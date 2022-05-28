package alien.api.catalogue;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
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
public class GetTagValues extends Request {
	private static final long serialVersionUID = -8097151852196189205L;

	private final String path;
	private final String tag;
	private final boolean includeParents;
	private final Set<String> columnConstraints;

	private Map<String, String> tagValues;

	/**
	 * @param user
	 * @param path path to query
	 * @param tag tag name
	 * @param columnConstraints restrict the return values to the given set of columns
	 */
	public GetTagValues(final AliEnPrincipal user, final String path, final String tag, final Set<String> columnConstraints) {
		setRequestUser(user);
		this.path = path;
		this.tag = tag;
		this.includeParents = true;
		this.columnConstraints = columnConstraints;
	}

	@Override
	public List<String> getArguments() {
		return Arrays.asList(this.path, this.tag, String.valueOf(includeParents), columnConstraints != null ? columnConstraints.toString() : "null");
	}

	@Override
	public void run() {
		this.tagValues = LFNUtils.getTagValues(path, tag, includeParents, columnConstraints);
	}

	/**
	 * @return the requested columns for this tag on the given path
	 */
	public Map<String, String> getTagValues() {
		return this.tagValues;
	}

	@Override
	public String toString() {
		return "Asked for : " + this.path + ", tag " + tag + ", reply is: " + this.tagValues;
	}
}
