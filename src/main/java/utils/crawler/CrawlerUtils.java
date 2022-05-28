package utils.crawler;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Predicate;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import alien.catalogue.LFN;
import alien.config.ConfigUtils;
import alien.io.protocols.SourceExceptionCode;
import alien.se.SE;
import alien.shell.commands.JAliEnCOMMander;
import utils.StatusCode;
import utils.StatusType;

/**
 * @author anegru
 */
public class CrawlerUtils {
	/**
	 * Logger
	 */
	private static final Logger logger = ConfigUtils.getLogger(CrawlerUtils.class.getCanonicalName());

	/**
	 * @return list of StatusCode objects that represent all possible statuses returned by the crawler
	 */
	public static List<StatusCode> getStatuses() {
		final List<StatusCode> crawlingStatuses = new ArrayList<>();
		crawlingStatuses.addAll(Arrays.asList(SourceExceptionCode.values()));
		crawlingStatuses.addAll(Arrays.asList(CrawlingStatusCode.values()));
		return Collections.unmodifiableList(crawlingStatuses);
	}

	/**
	 * @return list of all possible status types returned by the crawler
	 */
	public static List<String> getStatusTypes() {
		return Arrays.stream(StatusType.values()).map(Enum::toString).collect(Collectors.toUnmodifiableList());
	}

	/**
	 * Return the storage element name to be used in a Grid path
	 * @param se
	 * @return String
	 */
	public static String getSEName(final SE se) {
		return se.seName.replace("::", "_");
	}

	/**
	 * Upload a file f to the Grid at the remoteFullPath
	 * @param commander
	 * @param f
	 * @param remoteFullPath
	 * @throws IOException
	 */
	public static void uploadToGrid(final JAliEnCOMMander commander, final File f, final String remoteFullPath) throws IOException {

		logger.log(Level.INFO, "Uploading " + remoteFullPath);

		final LFN lfnUploaded = commander.c_api.uploadFile(f, remoteFullPath, "-w", "-T", "2", "-d");

		if (lfnUploaded == null)
			logger.log(Level.WARNING, "Uploading " + remoteFullPath + " failed");
		else
			logger.log(Level.INFO, "Successfully uploaded " + remoteFullPath);
	}

	/**
	 * @param commander
	 * @param contents
	 * @param localFileName
	 * @param remoteFullPath
	 */
	public static void uploadToGrid(final JAliEnCOMMander commander, final String contents, final String localFileName, final String remoteFullPath) {
		final File f = new File(localFileName);
		try (BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(f))) {
			bufferedWriter.write(contents);
			bufferedWriter.flush();
			bufferedWriter.close();
			uploadToGrid(commander, f, remoteFullPath);
		}
		catch (final IOException e) {
			e.printStackTrace();
			logger.log(Level.SEVERE, "Cannot write to disk " + e.getMessage());
		}
		finally {
			try {
				if (f.exists() && !f.delete())
					logger.log(Level.INFO, "Cannot delete already existing local file " + f.getCanonicalPath());
			}
			catch (final Exception e) {
				e.printStackTrace();
				logger.log(Level.WARNING, "Cannot delete already existing local file " + e.getMessage());
			}
		}
	}

	/**
	 * Get the full list of SEs that have to be crawled. Only SEs with type 'disk' are selected
	 *
	 * @param commander
	 *
	 * @return List<SE>
	 * @throws Exception
	 */
	public static List<SE> getStorageElementsForCrawling(final JAliEnCOMMander commander) throws Exception {
		final Collection<SE> ses = commander.c_api.getSEs(new ArrayList<>());

		if (ses == null)
			throw new Exception("Cannot retrieve SEs");

		final Predicate<SE> byType = se -> se.isQosType("disk");
		return ses.stream().filter(byType).collect(Collectors.toList());
	}

	/**
	 * Return the path to the SE given as parameter for the current iteration
	 *
	 * @param commander
	 *
	 * @param se The SE for which to get the directory path
	 * @param iterationUnixTimestamp
	 * @return String
	 */
	public static String getSEIterationDirectoryPath(final JAliEnCOMMander commander, final SE se, final String iterationUnixTimestamp) {
		return getIterationDirectoryPath(commander, iterationUnixTimestamp) + CrawlerUtils.getSEName(se) + "/";
	}

	/**
	 * Return the path of the current iteration
	 *
	 * @return String
	 */
	private static String getIterationDirectoryPath(final JAliEnCOMMander commander, final String iterationUnixTimestamp) {
		return commander.getCurrentDirName() + "iteration_" + iterationUnixTimestamp + "/";
	}
}
