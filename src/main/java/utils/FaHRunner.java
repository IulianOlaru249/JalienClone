/**
 *
 */
package utils;

import java.io.File;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

import alien.api.Dispatcher;
import alien.api.taskQueue.FaHTask;
import alien.catalogue.LFN;
import alien.catalogue.access.AuthorizationFactory;
import alien.config.ConfigUtils;
import alien.monitoring.Monitor;
import alien.monitoring.MonitorFactory;
import alien.shell.commands.JAliEnCOMMander;
import alien.user.AliEnPrincipal;
import alien.user.JAKeyStore;
import alien.user.UsersHelper;

/**
 * @author costing
 * @since Mar 31, 2020
 */
public class FaHRunner {

	/**
	 * logger
	 */
	static final Logger logger = ConfigUtils.getLogger(FaHRunner.class.getCanonicalName());

	/**
	 * ML monitor object
	 */
	static final Monitor monitor = MonitorFactory.getMonitor(FaHRunner.class.getCanonicalName());

	/**
	 * Entry point where the Folding@Home job starts
	 *
	 * @param args
	 * @throws Exception
	 */
	public static void main(final String[] args) throws Exception {
		ConfigUtils.setApplicationName("FaH");
		ConfigUtils.switchToForkProcessLaunching();

		if (!JAKeyStore.loadKeyStore()) {
			logger.log(Level.SEVERE, "No identity found, exiting");
			return;
		}

		final AliEnPrincipal account = AuthorizationFactory.getDefaultUser();

		final long currentJobID = ConfigUtils.getConfig().getl("ALIEN_PROC_ID", -1);

		if (currentJobID < 0) {
			logger.log(Level.SEVERE, "Cannot get the job ID, exiting");
			return;
		}

		final FaHTask task = Dispatcher.execute(new FaHTask(currentJobID));

		if (task.getSequenceId() < 0) {
			logger.log(Level.WARNING, "I was not assigned a slot, exiting");
			return;
		}

		final String baseFolder = UsersHelper.getDefaultUserDir(account.getDefaultUser() + "/fah/" + task.getSequenceId());

		logger.log(Level.INFO, "I was assigned this slot: " + baseFolder);

		final String snapshotArchive = baseFolder + "/snapshot.tar.gz";

		final JAliEnCOMMander commander = JAliEnCOMMander.getInstance();

		final LFN lSnapshot = commander.c_api.getLFN(snapshotArchive);

		if (lSnapshot != null) {
			logger.log(Level.INFO, "A previous snapshot exists: " + lSnapshot.getCanonicalName());

			try {
				commander.c_api.downloadFile(snapshotArchive, new File("snapshot.tar.gz"));

				logger.log(Level.INFO, "Snapshot of " + lSnapshot.getSize() + " bytes was downloaded successfully");
			}
			catch (final IOException ioe) {
				logger.log(Level.WARNING, "Snapshot cannot be retrieved but I will continue with an empty slot anyway", ioe);
			}
		}

		try {
			final ProcessBuilder pBuilder = new ProcessBuilder("./fah.sh");

			pBuilder.redirectErrorStream(true);
			pBuilder.redirectOutput(new File("fah.log"));

			final Process p = pBuilder.start();
			p.waitFor();
		}
		catch (final IOException ioe) {
			logger.log(Level.INFO, "I couldn't run the payload, execution of ./fah.sh failed with:\n" + ioe.getMessage());
			return;
		}

		// ok, now we have to upload the results, if any
		final File outputSnapshot = new File("snapshot.tar.gz");
		final File logFile = new File("log.txt");

		if (logFile.exists())
			logger.log(Level.INFO, "Job produced a log file, it was probably ok");
		else {
			logger.log(Level.SEVERE, "Job didn't run correctly, no log.txt file found");
			return;
		}

		if (outputSnapshot.exists()) {
			logger.log(Level.INFO, "Uploading intermediate work to " + outputSnapshot);

			if (lSnapshot != null) {
				if (commander.c_api.removeLFN(snapshotArchive))
					logger.log(Level.INFO, "Removal of previous archive was successful");
				else
					logger.log(Level.WARNING, "Could not remove the previous archive, has somebody removed it while we were running?");
			}

			logger.log(Level.INFO, "Uploading " + outputSnapshot.length() + " bytes");
			commander.c_api.uploadFile(outputSnapshot, snapshotArchive, "-w", "-T", "2", "-d");
			logger.log(Level.INFO, "Upload complete");
		}
		else
			logger.log(Level.INFO, "No output to upload");
	}
}
