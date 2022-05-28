package alien.api.catalogue;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import alien.api.Request;
import alien.catalogue.PFN;
import alien.se.SEUtils;
import alien.user.AliEnPrincipal;

/**
 * @author anegru
 * @since 2020-03-2
 */
public class SERandomPFNs extends Request {

	/**
	 * Generated value
	 */
	private static final long serialVersionUID = 5169310474130045510L;

	private final int seNumber;
	private final int fileCount;

	private Collection<PFN> randomPFNs = null;

	/**
	 * @param user
	 * @param seNumber SE number to extract random PFNs from
	 * @param fileCount number of PFNs to return at max
	 */
	public SERandomPFNs(final AliEnPrincipal user, final int seNumber, final int fileCount) {
		setRequestUser(user);

		this.seNumber = seNumber;
		this.fileCount = fileCount;
	}

	@Override
	public List<String> getArguments() {
		return Arrays.asList(String.valueOf(this.seNumber), String.valueOf(this.fileCount));
	}

	@Override
	public void run() {
		this.randomPFNs = SEUtils.getRandomPFNs(this.seNumber, this.fileCount);
	}

	/**
	 * @return the random PFNs
	 */
	public Collection<PFN> getPFNs() {
		return randomPFNs;
	}
}
