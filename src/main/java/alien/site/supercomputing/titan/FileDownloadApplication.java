package alien.site.supercomputing.titan;

import java.util.LinkedList;
import java.util.List;

import alien.catalogue.LFN;

/**
 * @author psvirin
 *
 */
public class FileDownloadApplication {
	/**
	 * List of LFNs to download
	 */
	List<LFN> fileList;
	private List<Pair<LFN, String>> dlResult;

	/**
	 * @param inputFiles
	 */
	FileDownloadApplication(final List<LFN> inputFiles) {
		fileList = inputFiles;
		dlResult = new LinkedList<>();
	}

	/**
	 * @param l
	 * @param s
	 */
	synchronized void putResult(final LFN l, final String s) {
		// System.out.println("Really putting: " + s);
		final Pair<LFN, String> p = new Pair<>(l, s);
		// System.out.println("Really put: " + p.getSecond());
		dlResult.add(p);
	}

	/**
	 * @return <code>true</code> if all files were downloaded
	 */
	public synchronized boolean isCompleted() {
		return fileList.size() == dlResult.size();
	}

	/**
	 * @return pairs of LFN to local file path
	 */
	public final List<Pair<LFN, String>> getResults() {
		return dlResult;
	}

	/**
	 *
	 */
	synchronized public void print() {
		// System.out.println("=================: " + this);
		// System.out.println("Ordered: ");
		for (final LFN l : fileList)
			System.out.println(l.getCanonicalName());
		// System.out.println("Downloaded: ");
		for (final Pair<LFN, String> p : dlResult)
			System.out.println(p.getFirst().getCanonicalName() + ": " + p.getSecond());
	}
}
