package alien.site.supercomputing.titan;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import alien.api.catalogue.CatalogueApiUtils;
import alien.catalogue.GUID;
import alien.catalogue.LFN;
import alien.catalogue.PFN;
import alien.io.IOUtils;
import alien.shell.commands.JAliEnCOMMander;

/**
 * @author psvirin
 *
 */
public class FileDownloadController extends Thread {
	private final HashMap<LFN, LinkedList<FileDownloadApplication>> lfnRequested;
	// private Set<LFN> lfnQueueInProcess;

	private final BlockingQueue<LFN> lfnToServe;

	private static String cacheFolder;
	private final int maxParallelDownloads = 10;

	private final JAliEnCOMMander commander = JAliEnCOMMander.getInstance();
	private final CatalogueApiUtils c_api = new CatalogueApiUtils(commander);
	private final ArrayList<Thread> dlPool;

	static private FileDownloadController instance = null;

	/**
	 * @param path
	 *            base path
	 */
	public static void setCacheFolder(final String path) {
		if (path != null && path.length() > 0)
			cacheFolder = path;
	}

	/**
	 * The class which does actual download of LFNs
	 */
	class FileDownloadThread extends Thread {
		@Override
		public void run() {
			while (true)
				try {
					// if something is present in the
					final LFN l = lfnToServe.take();
					String dlFilename;
					if (l != null) {
						dlFilename = runDownload(l);
						// notify download finished
						notifyCompleted(l, dlFilename);
					}
				}
				catch (@SuppressWarnings("unused") final InterruptedException e) {
					continue;
				}
		}

		private String runDownload(final LFN l) {
			if (fileIsInCache(l)) {
				System.out.println("File is present in cache: " + getCachedFilename(l));
				return getCachedFilename(l);
			}
			System.out.println("File is not present in cache: " + getCachedFilename(l));
			final List<PFN> pfns = c_api.getPFNsToRead(l, null, null);

			if (pfns == null || pfns.size() == 0) {
				System.out.println("No replicas of " + l.getCanonicalName() + " to read from");
				return null;
			}

			final GUID g = pfns.iterator().next().getGuid();
			// commander.q_api.putJobLog(queueId, "trace", "Getting InputFile: " +
			// entry.getKey().getCanonicalName());
			// final File f = IOUtils.get(g, entry.getValue());
			final String dstFilename = getCachedFilename(l);
			System.out.println("Downloading to " + dstFilename);
			createCacheFolders(dstFilename);

			final StringBuilder errorMessage = new StringBuilder();

			final File f = IOUtils.get(g, new File(dstFilename), errorMessage);

			if (f == null) {
				// System.out.println("Could not download " + entry.getKey().getCanonicalName() +
				// " to " + entry.getValue().getAbsolutePath());
				System.out.println("Could not download " + l.getCanonicalName() + " to " + dstFilename + ", due to:\n" + errorMessage);
				return null;
			}

			// return f.getName();
			return dstFilename;
		}
	}

	/**
	 * @return singleton
	 */
	static synchronized FileDownloadController getInstance() {
		try {
			if (instance == null)
				instance = new FileDownloadController();
		}
		catch (final Exception e) {
			System.err.println("Exception caught on starting FileDownloadController: " + e.getMessage());
			return null;
		}
		return instance;
	}

	// private FileDownloadController(String cacheFolder) throws IOException, FileNotFoundException{
	private FileDownloadController() throws IOException, FileNotFoundException {
		if (cacheFolder == null || "".equals(cacheFolder))
			throw new IOException("Cache folder name can not be null");

		lfnRequested = new HashMap<>();
		lfnToServe = new LinkedBlockingQueue<>();

		// here to start a pool of threads
		dlPool = new ArrayList<>(maxParallelDownloads);
		for (int i = maxParallelDownloads; i > 0; i--) {
			final FileDownloadThread fdt = new FileDownloadThread();
			dlPool.add(fdt);
			fdt.start();
		}
		this.start();

	}

	/**
	 * @param inputFiles
	 * @return
	 */
	synchronized FileDownloadApplication applyForDownload(final List<LFN> inputFiles) {
		final FileDownloadApplication fda = new FileDownloadApplication(inputFiles);
		for (final LFN l : inputFiles)
			if (lfnRequested.get(l) == null) {
				final LinkedList<FileDownloadApplication> dlAppList = new LinkedList<>();
				dlAppList.add(fda);
				lfnRequested.put(l, dlAppList);
				try {
					lfnToServe.put(l);
				}
				catch (@SuppressWarnings("unused") final InterruptedException e) {
					break;
				}
			}
			else
				lfnRequested.get(l).add(fda);

		return fda;
	}

	private synchronized void notifyCompleted(final LFN l, final String filename) {
		for (final FileDownloadApplication fda : lfnRequested.get(l)) {
			System.out.println("Putting " + filename + " to FDA: " + fda);
			fda.putResult(l, filename);
			fda.print();
			if (filename == null)
				for (final LFN lr : fda.fileList) {
					System.out.println("Notify completed explanation: ");
					System.out.println(lr);
					System.out.println(lfnRequested.get(lr));
					lfnRequested.get(lr).remove(fda);
				}
			if (fda.isCompleted() || filename == null)
				notifyCompletedFDA(fda);
			lfnRequested.remove(l);
		}
	}

	private static void notifyCompletedFDA(final FileDownloadApplication fda) {
		synchronized (fda) {
			fda.notifyAll();
		}
	}

	private static boolean fileIsInCache(final LFN l) {
		return new File(getCachedFilename(l)).exists();
	}

	private static String getCachedFilename(final LFN l) {
		return cacheFolder + "/" + l.getCanonicalName();
	}

	/*
	 * private static boolean checkMd5() {
	 * return true;
	 * }
	 */

	/*
	 * private boolean checkSize(final LFN l) throws IOException {
	 * File f = new File(getCachedFilename(l));
	 * return l.size == f.length();
	 * }
	 */

	private static void createCacheFolders(final String f) {
		final File file = new File(f);
		file.getParentFile().mkdirs();
	}
}
