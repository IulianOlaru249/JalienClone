package alien.api.catalogue;

import java.util.Arrays;
import java.util.Collection;
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
public class FindCsdfromString extends Request {

	private static final long serialVersionUID = -5938936122293608584L;
	private final String path;
	private final String pattern;
	private final String metadata;
	private final int flags;
	private Collection<LFN_CSD> lfns;
	// private final String xmlCollectionName; // TODO filter jobid and create and upload XML collections
	// private Long queueid = Long.valueOf(0);

	// /**
	// * @param user
	// * @param path
	// * @param pattern
	// * @param flags
	// */
	// public FindCsdfromString(final AliEnPrincipal user, final String path, final String pattern, final int flags) {
	// setRequestUser(user);
	// this.path = path;
	// this.pattern = pattern;
	// this.query = null;
	// this.flags = flags;
	// // this.xmlCollectionName = "";
	// }

	/**
	 * @param user
	 * @param path
	 * @param pattern
	 * @param metadata
	 * @param flags
	 */
	public FindCsdfromString(final AliEnPrincipal user, final String path, final String pattern, final String metadata, final int flags) {
		setRequestUser(user);
		this.path = path;
		this.pattern = pattern;
		this.metadata = metadata;
		this.flags = flags;
		// this.xmlCollectionName = xmlCollectionName;
		// this.queueid = queueid;
	}

	@Override
	public List<String> getArguments() {
		return Arrays.asList(this.path, this.pattern, this.metadata, String.valueOf(this.flags));
	}

	// /**
	// * @param user
	// * @param path
	// * @param pattern
	// * @param flags
	// * @param xmlCollectionName
	// */
	// public FindCsdfromString(final AliEnPrincipal user, final String path, final String pattern, final int flags, final String xmlCollectionName) {
	// setRequestUser(user);
	// this.path = path;
	// this.pattern = pattern;
	// this.query = null;
	// this.flags = flags;
	// // this.xmlCollectionName = xmlCollectionName;
	// }

	@Override
	public void run() {
		lfns = LFNCSDUtils.find(path, pattern, flags, metadata);
	}

	/**
	 * @return the found LFNs
	 */
	public Collection<LFN_CSD> getLFNs() {
		return this.lfns;
	}

	@Override
	public String toString() {
		return "Asked for : path (" + this.path + "), pattern (" + this.pattern + "), flags (" + this.flags + ") reply is:\n" + this.lfns;
	}
}
