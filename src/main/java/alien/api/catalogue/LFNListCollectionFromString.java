package alien.api.catalogue;

import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import alien.api.Request;
import alien.catalogue.LFN;
import alien.catalogue.LFNUtils;
import alien.catalogue.XmlCollection;
import alien.user.AliEnPrincipal;

/**
 * Get the LFN object for this path
 *
 * @author ron
 * @since Jun 08, 2011
 */
public class LFNListCollectionFromString extends Request {

	/**
	 *
	 */
	private static final long serialVersionUID = -7167353294190733455L;
	private final String path;

	private Set<LFN> lfns = null;

	private boolean onlyAliEnCollections = false;

	/**
	 * @param user
	 * @param path
	 * @param onlyAliEnCollections validate if the given path is an AliEn collection, throw an exception if not
	 */
	public LFNListCollectionFromString(final AliEnPrincipal user, final String path, final boolean onlyAliEnCollections) {
		setRequestUser(user);
		this.path = path;
		this.onlyAliEnCollections = onlyAliEnCollections;
	}

	@Override
	public List<String> getArguments() {
		return Arrays.asList(this.path, String.valueOf(this.onlyAliEnCollections));
	}

	@Override
	public void run() {
		final LFN entry = LFNUtils.getLFN(path, false);

		if (entry != null) {
			if (entry.isCollection()) {
				final Set<String> entries = entry.listCollection();

				this.lfns = new LinkedHashSet<>(entries.size());

				for (final String lfn : entries)
					this.lfns.add(LFNUtils.getLFN(lfn));

				return;
			}

			if (onlyAliEnCollections)
				throw new IllegalArgumentException("This entry is not an AliEn collection: \"" + path + "\"");

			if (entry.isFile()) {
				// is it an XML collection ?
				try {
					this.lfns = new XmlCollection(entry);
				}
				catch (final IOException ioe) {
					throw new IllegalArgumentException(ioe.getMessage() + " (" + path + ")");
				}
			}
			else {
				throw new IllegalArgumentException("Illegal entry type for \"" + path + "\": " + entry.getType());
			}
		}
		else
			throw new IllegalArgumentException("No such LFN \"" + path + "\"");
	}

	/**
	 * @return the requested LFN
	 */
	public Set<LFN> getLFNs() {
		return this.lfns;
	}

	@Override
	public String toString() {
		return "Asked for : " + this.path + " reply " + (this.lfns != null ? "has " + this.lfns.size() + " entres" : "is null");
	}
}
