package alien.site;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.lang.ProcessBuilder.Redirect;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.Vector;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import alien.api.TomcatServer;
import alien.api.catalogue.CatalogueApiUtils;
import alien.api.taskQueue.TaskQueueApiUtils;
import alien.catalogue.FileSystemUtils;
import alien.catalogue.LFN;
import alien.catalogue.XmlCollection;
import alien.config.ConfigUtils;
import alien.io.IOUtils;
import alien.monitoring.Monitor;
import alien.monitoring.MonitorFactory;
import alien.monitoring.MonitoringObject;
import alien.monitoring.Timing;
import alien.shell.commands.JAliEnCOMMander;
import alien.shell.commands.JAliEnCommandcp;
import alien.site.packman.CVMFS;
import alien.site.packman.PackMan;
import alien.taskQueue.JDL;
import alien.taskQueue.JobStatus;
import alien.user.JAKeyStore;
import alien.user.UserFactory;
import apmon.ApMon;
import lazyj.Format;
import lia.util.process.ExternalProcesses;

/**
 * Job execution wrapper, running an embedded Tomcat server for in/out-bound communications
 */
public final class JobWrapper implements MonitoringObject, Runnable {

	// Folders and files
	private final File currentDir = new File(Paths.get(".").toAbsolutePath().normalize().toString());
	private final String tmpDir = currentDir + "/tmp";
	private final String timeFile = ".jalienTimes";
	private final String jobstatusFile = ".jalienJobstatus";
	private String defaultOutputDirPrefix;

	// Job variables
	/**
	 * @uml.property name="jdl"
	 * @uml.associationEnd
	 */
	private JDL jdl;
	private long queueId;
	private int resubmission;
	private String username;
	private String tokenCert;
	private String tokenKey;
	private HashMap<String, Object> siteMap;
	private String ce;
	private String legacyToken;
	private long ttl;
	/**
	 * @uml.property name="jobStatus"
	 * @uml.associationEnd
	 */
	private JobStatus jobStatus;

	private final Long masterjobID;

	// Other
	/**
	 * @uml.property name="packMan"
	 * @uml.associationEnd
	 */
	private final PackMan packMan;
	private final String hostName;
	private final int pid;
	private final String ceHost;
	private final String parentHostname;
	private final Map<String, String> metavars;
	/**
	 * @uml.property name="commander"
	 * @uml.associationEnd
	 */
	private final JAliEnCOMMander commander;

	/**
	 * @uml.property name="c_api"
	 * @uml.associationEnd
	 */
	private final CatalogueApiUtils c_api;

	/**
	 * logger object
	 */
	static final Logger logger = ConfigUtils.getLogger(JobWrapper.class.getCanonicalName());

	/**
	 * Streams for data transfer
	 */
	private ObjectInputStream inputFromJobAgent;

	/**
	 * ML monitor object
	 */
	static final Monitor monitor = MonitorFactory.getMonitor(JobAgent.class.getCanonicalName());

	/**
	 * ApMon sender
	 */
	static final ApMon apmon = MonitorFactory.getApMonSender();

	/**
	 * Payload process
	 */
	private Process payload;

	private final Thread statusSenderThread = new Thread("JobWrapper.statusSenderThread") {
		@Override
		public void run() {
			if (apmon == null)
				return;

			while (!jobKilled) {
				final Vector<String> paramNames = new Vector<>(5);
				final Vector<Object> paramValues = new Vector<>(5);

				paramNames.add("host_pid");
				paramValues.add(Double.valueOf(MonitorFactory.getSelfProcessID()));

				if (username != null) {
					paramNames.add("job_user");
					paramValues.add(username);
				}

				paramNames.add("host");
				paramValues.add(ConfigUtils.getLocalHostname());

				if (jobStatus != null) {
					paramNames.add("status");
					paramValues.add(Double.valueOf(jobStatus.getAliEnLevel()));
				}

				if (masterjobID != null) {
					paramNames.add("masterjob_id");
					paramValues.add(Double.valueOf(masterjobID.longValue()));
				}

				try {
					apmon.sendParameters(ce + "_Jobs", String.valueOf(queueId), paramNames.size(), paramNames, paramValues);
				}
				catch (final Exception e) {
					logger.log(Level.WARNING, "Cannot send status updates to ML", e);
				}

				synchronized (this) {
					try {
						wait(1000 * 60);
					}
					catch (@SuppressWarnings("unused") final InterruptedException e) {
						return;
					}
				}
			}
		}
	};

	/**
	 * @throws Exception anything bad happening during startup
	 */
	@SuppressWarnings("unchecked")
	public JobWrapper() throws Exception {

		pid = MonitorFactory.getSelfProcessID();

		try {
			inputFromJobAgent = new ObjectInputStream(System.in);
			jdl = (JDL) inputFromJobAgent.readObject();
			username = (String) inputFromJobAgent.readObject();
			queueId = ((Long) inputFromJobAgent.readObject()).longValue();
			resubmission = ((Integer) inputFromJobAgent.readObject()).intValue();
			tokenCert = (String) inputFromJobAgent.readObject();
			tokenKey = (String) inputFromJobAgent.readObject();
			ce = (String) inputFromJobAgent.readObject();
			siteMap = (HashMap<String, Object>) inputFromJobAgent.readObject();
			defaultOutputDirPrefix = (String) inputFromJobAgent.readObject();
			legacyToken = (String) inputFromJobAgent.readObject();
			ttl = ((Long) inputFromJobAgent.readObject()).longValue();
			parentHostname = (String) inputFromJobAgent.readObject();
			metavars = (HashMap<String, String>) inputFromJobAgent.readObject();

			if (logger.isLoggable(Level.FINEST)) {
				logger.log(Level.FINEST, "We received the following tokenCert: " + tokenCert);
				logger.log(Level.FINEST, "We received the following tokenKey: " + tokenKey);
			}

			logger.log(Level.INFO, "We received the following username: " + username);
			logger.log(Level.INFO, "We received the following CE " + ce);

			masterjobID = jdl.getLong("MasterjobID");
		}
		catch (final IOException | ClassNotFoundException e) {
			logger.log(Level.SEVERE, "Error: Could not receive data from JobAgent" + e);
			throw e;
		}

		if ((tokenCert != null) && (tokenKey != null)) {
			try {
				JAKeyStore.createTokenFromString(tokenCert, tokenKey);
				logger.log(Level.INFO, "Token successfully created");
				JAKeyStore.loadKeyStore();
			}
			catch (final Exception e) {
				logger.log(Level.SEVERE, "Error. Could not load tokenCert and/or tokenKey" + e);
				throw e;
			}
		}

		hostName = (String) Objects.requireNonNullElse(siteMap.get("Host"), "");
		ceHost = (String) Objects.requireNonNullElse(siteMap.get("CEhost"), hostName);
		packMan = (PackMan) Objects.requireNonNullElse(siteMap.get("PackMan"), new CVMFS(""));

		commander = JAliEnCOMMander.getInstance();
		c_api = new CatalogueApiUtils(commander);

		// use same tmpdir everywhere
		System.setProperty("java.io.tmpdir", tmpDir);

		statusSenderThread.setDaemon(true);
		statusSenderThread.start();

		logger.log(Level.INFO, "JobWrapper initialised. Running as the following user: " + commander.getUser().getName());

		try {
			final String osRelease = Files.readString(Paths.get("/etc/os-release"));
			final String osName = osRelease.substring(osRelease.indexOf("PRETTY_NAME=") + 12, osRelease.length()).split("\\r?\\n")[0];

			putJobTrace("The following OS has been detected: " + osName);
			logger.log(Level.INFO, "The following OS has been detected: " + osName);
		}
		catch (@SuppressWarnings("unused") final IOException e1) {
			// Ignore
		}

		monitor.addMonitoring("JobWrapper", this);
	}

	@Override
	public void run() {

		logger.log(Level.INFO, "Starting JobWrapper in " + hostName);

		// We start, if needed, the node Tomcat server
		// Does it check a previous one is already running?
		try {
			logger.log(Level.INFO, "Trying to start Tomcat");
			TomcatServer.startTomcatServer();
		}
		catch (final Exception e) {
			logger.log(Level.WARNING, "Unable to start Tomcat." + e);
		}

		logger.log(Level.INFO, "Tomcat started");

		// process payload
		final int runCode = runJob();

		logger.log(Level.INFO, "JobWrapper has finished execution");
		putJobTrace("JobWrapper has finished execution");

		if (runCode > 0)
			System.exit(0); // Positive runCodes originate from the payload. Ignore. All OK here as far as we're concerned.
		else
			System.exit(Math.abs(runCode));
	}

	private Map<String, String> installPackages(final ArrayList<String> packToInstall) {
		if (packMan == null) {
			logger.log(Level.WARNING, "Packman is null!");
			return null;
		}

		try (Timing t = new Timing()) {
			final Map<String, String> env = packMan.installPackage(username, String.join(",", packToInstall), null);
			if (env == null) {
				logger.log(Level.SEVERE, "Error installing " + packToInstall);
				putJobTrace("Error setting the environment for " + packToInstall);
				System.exit(1);
			}

			logger.log(Level.INFO, "It took " + t + " to generate the environment for " + packToInstall);

			return env;
		}
	}

	private class PackagesResolver extends Thread {
		private Map<String, String> environment_packages;

		@Override
		public void run() {
			environment_packages = getJobPackagesEnvironment();
		}
	}

	private class InputFilesDownloader extends Thread {
		private boolean downloadedOk;

		@Override
		public void run() {
			downloadedOk = getInputFiles();
		}
	}

	private int runJob() {
		try {
			logger.log(Level.INFO, "Started JobWrapper for: " + jdl);

			changeStatus(JobStatus.STARTED);

			final PackagesResolver packResolver = new PackagesResolver();
			packResolver.start();

			final InputFilesDownloader downloader = new InputFilesDownloader();
			downloader.start();

			downloader.join();
			packResolver.join();

			if (!downloader.downloadedOk) {
				logger.log(Level.SEVERE, "Failed to get inputfiles");
				changeStatus(JobStatus.ERROR_IB);
				return -1;
			}

			// run payload
			final int execExitCode = execute(packResolver.environment_packages);

			getTraceFromFile();

			if (execExitCode != 0) {
				logger.log(Level.SEVERE, "Failed to run payload");

				if (execExitCode < 0)
					putJobTrace("Failed to start execution of payload. Exit code: " + Math.abs(execExitCode));
				else
					putJobTrace("Warning: executable exit code was " + execExitCode);

				return uploadOutputFiles(JobStatus.ERROR_E, execExitCode) ? execExitCode : -1;
			}

			final int valExitCode = validate(packResolver.environment_packages);

			getTraceFromFile();

			if (valExitCode != 0) {
				logger.log(Level.SEVERE, "Validation failed");

				if (valExitCode < 0)
					putJobTrace("Failed to start validation. Exit code: " + Math.abs(valExitCode));
				else
					putJobTrace("Validation failed. Exit code: " + valExitCode);

				final int valUploadExitCode = uploadOutputFiles(JobStatus.ERROR_V, valExitCode) ? valExitCode : -1;

				return valUploadExitCode;
			}

			if (!uploadOutputFiles(JobStatus.DONE)) {
				logger.log(Level.SEVERE, "Failed to upload output files");
				return -1;
			}

			cleanupProcesses(queueId, pid);

			return 0;
		}
		catch (final Exception e) {
			logger.log(Level.SEVERE, "Unable to handle job", e);
			final StringBuilder sb = new StringBuilder("ERROR! Unable to handle job: " + e + "\n\r");
			for (final StackTraceElement elem : e.getStackTrace()) {
				sb.append(elem);
				sb.append("\n\r");
			}
			putJobTrace(sb.toString());
			return -1;
		}
	}

	/**
	 * @param command
	 * @param arguments
	 * @param timeout
	 * @return <code>0</code> if everything went fine, a positive number with the process exit code (which would mean a problem) and a negative error code in case of timeout or other supervised
	 *         execution errors
	 */
	private int executeCommand(final String command, final List<String> arguments, final Map<String, String> environment_packages, final String executionType) {

		logger.log(Level.INFO, "Starting execution of command: " + command);

		final List<String> cmd = new LinkedList<>();

		boolean trackTime = false;
		try {
			final String supportsTime = ExternalProcesses.getCmdOutput(Arrays.asList("time", "echo"), true, 30L, TimeUnit.SECONDS);
			if (!supportsTime.contains("command not found"))
				trackTime = true;
		}
		catch (@SuppressWarnings("unused") final Exception e) {
			// ignore
		}

		if (trackTime) {
			cmd.add("/usr/bin/time");
			cmd.add("-p");
			cmd.add("-o");
			cmd.add(tmpDir + "/" + timeFile + "-" + executionType);
		}

		final int idx = command.lastIndexOf('/');

		final String cmdStrip = idx < 0 ? command : command.substring(idx + 1);

		final File fExe = new File(currentDir, cmdStrip);

		if (!fExe.exists()) {
			logger.log(Level.SEVERE, "ERROR. Executable was not found");
			return -2;
		}

		fExe.setExecutable(true);

		cmd.add(fExe.getAbsolutePath());

		if (arguments != null && arguments.size() > 0)
			for (final String argument : arguments)
				if (argument.trim().length() > 0) {
					final StringTokenizer st = new StringTokenizer(argument);

					while (st.hasMoreTokens())
						cmd.add(st.nextToken());
				}

		// Check if we can put the payload in its own container
		// TODO: Put back later
		// Containerizer cont = ContainerizerFactory.getContainerizer();
		// if (cont != null) {
		// monitor.sendParameter("containerLayer", Integer.valueOf(2));
		// cmd = cont.containerize(String.join(" ", cmd));
		// }

		logger.log(Level.INFO, "Executing: " + cmd + ", arguments is " + arguments + " pid: " + pid);

		final ProcessBuilder pBuilder = new ProcessBuilder(cmd);

		final Map<String, String> processEnv = pBuilder.environment();
		final HashMap<String, String> jBoxEnv = ConfigUtils.exportJBoxVariables();

		processEnv.putAll(environment_packages);
		processEnv.putAll(loadJDLEnvironmentVariables());
		processEnv.putAll(jBoxEnv);
		processEnv.put("JALIEN_TOKEN_CERT", tokenCert);
		processEnv.put("JALIEN_TOKEN_KEY", tokenKey);
		processEnv.put("ALIEN_JOB_TOKEN", legacyToken); // add legacy token
		processEnv.put("ALIEN_PROC_ID", String.valueOf(queueId));
		processEnv.put("ALIEN_MASTERJOB_ID", String.valueOf(masterjobID != null ? masterjobID.longValue() : queueId));
		processEnv.put("ALIEN_SITE", siteMap.get("Site").toString());
		processEnv.put("ALIEN_USER", username);

		processEnv.put("HOME", currentDir.getAbsolutePath());
		processEnv.put("TMP", currentDir.getAbsolutePath() + "/tmp");
		processEnv.put("TMPDIR", currentDir.getAbsolutePath() + "/tmp");

		processEnv.putAll(metavars);

		if (!parentHostname.isBlank())
			processEnv.put("PARENT_HOSTNAME", parentHostname);

		pBuilder.redirectOutput(Redirect.appendTo(new File(currentDir, "stdout")));
		pBuilder.redirectError(Redirect.appendTo(new File(currentDir, "stderr")));

		try {
			payload = pBuilder.start();

		}
		catch (final IOException ioe) {
			logger.log(Level.INFO, "Exception running " + cmd + " : " + ioe.getMessage());
			return -5;
		}

		if (!payload.isAlive()) {
			logger.log(Level.INFO, "The process for: " + cmd + " has terminated. Failed to execute?");
			return payload.exitValue();
		}

		try {
			sun.misc.Signal.handle(new sun.misc.Signal("TERM"), sig -> {
				if (payload.isAlive()) {
					logger.log(Level.SEVERE, "SIGTERM received. Killing payload");
					payload.destroyForcibly();
				}
			});

			payload.waitFor(ttl, TimeUnit.SECONDS);

			if (payload.isAlive()) {
				payload.destroyForcibly();
				logger.log(Level.SEVERE, "Payload process destroyed by timeout in wrapper!");
			}
			logger.log(Level.SEVERE, "Payload has finished execution.");
		}
		catch (final InterruptedException e) {
			logger.log(Level.INFO, "Interrupted while waiting for process to finish execution" + e);
		}

		if (trackTime) {
			try {
				putJobTrace("Execution completed. Time spent: " + Files.readString(Paths.get(tmpDir + "/" + timeFile + "-" + executionType)).replace("\n", ", "));
			}
			catch (@SuppressWarnings("unused") final Exception te) {
				// Ignore
			}
		}

		return payload.exitValue();
	}

	private int execute(final Map<String, String> environment_packages) {
		putJobTrace("Starting execution");

		changeStatus(JobStatus.RUNNING);
		final int code = executeCommand(jdl.gets("Executable"), jdl.getArguments(), environment_packages, "execution");

		return code;
	}

	private int validate(final Map<String, String> environment_packages) {
		int code = 0;

		final String validation = jdl.gets("ValidationCommand");

		if (validation != null) {
			putJobTrace("Starting validation");
			code = executeCommand(validation, null, environment_packages, "validation");
		}

		return code;
	}

	private boolean getInputFiles() {
		final Set<String> filesToDownload = new HashSet<>();

		List<String> list = jdl.getInputFiles(false);

		if (list != null)
			filesToDownload.addAll(list);

		list = jdl.getInputData(false);

		if (list != null)
			filesToDownload.addAll(list);

		String inputDataList = createInputDataList();

		String s = jdl.getExecutable();

		if (s != null)
			filesToDownload.add(s);

		s = jdl.gets("ValidationCommand");

		if (s != null)
			filesToDownload.add(s);

		final List<LFN> iFiles = c_api.getLFNs(filesToDownload, true, false);

		if (iFiles == null) {
			logger.log(Level.WARNING, "No requested files could be located");
			putJobTrace("ERROR: No requested files could be located: getLFNs returned null");
			return false;
		}

		if (iFiles.size() != filesToDownload.size()) {
			logger.log(Level.WARNING, "Not all requested files could be located");

			// diff
			while (!iFiles.isEmpty()) {
				filesToDownload.removeIf(slfn -> (slfn.contains(iFiles.get(iFiles.size() - 1).lfn)));
				iFiles.remove(iFiles.size() - 1);
			}
			putJobTrace("ERROR: Not all requested files could be located in the catalogue. Missing files: " + Arrays.toString(filesToDownload.toArray()));
			return false;
		}

		final Map<LFN, File> localFiles = new HashMap<>();

		for (final LFN l : iFiles) {
			File localFile = new File(currentDir, l.getFileName());

			int i = 0;

			while (localFile.exists() && i < 100000) {
				localFile = new File(currentDir, l.getFileName() + "." + i);
				i++;
			}

			if (localFile.exists()) {
				logger.log(Level.WARNING, "Too many occurences of " + l.getFileName() + " in " + currentDir.getAbsolutePath());
				putJobTrace("ERROR: Too many occurences of " + l.getFileName() + " in " + currentDir.getAbsolutePath());
				return false;
			}

			localFiles.put(l, localFile);
		}

		int duplicates = 0;
		for (final Map.Entry<LFN, File> entry : localFiles.entrySet()) {
			File f = entry.getValue();

			if (f.exists()) {
				duplicates++;
				f = new File(currentDir + "/" + duplicates, f.getName());
				f.mkdir();
				logger.log(Level.WARNING, "Warning: Could not download to " + entry.getValue().getAbsolutePath() + ". Already exists. Will instead use: " + f.getAbsolutePath());
				// putJobTrace("Warning: Could not download to " + entry.getValue().getAbsolutePath() + ". Already exists. Will instead use: " + f.getAbsolutePath());
			}

			if (inputDataList != null) {
				if (inputDataList.startsWith("<?xml"))
					inputDataList = inputDataList.replace("turl=\"alien://" + entry.getKey().getCanonicalName(), "turl=\"file:///" + f.getAbsolutePath()); // xmlcollection format here does not match AliEn
				else
					inputDataList = Format.replace(inputDataList, "alien://" + entry.getKey().getCanonicalName() + "\n", "file:///" + f.getAbsolutePath() + "\n");
			}

			putJobTrace("Getting InputFile: " + entry.getKey().getCanonicalName() + " to " + f.getAbsolutePath() + " (" + Format.size(entry.getKey().size) + ")");

			commander.clearLastError();

			final JAliEnCommandcp cp = new JAliEnCommandcp(commander, Arrays.asList(entry.getKey().getCanonicalName(), "file:" + f.getAbsolutePath()));

			final File copyResult = cp.copyGridToLocal(entry.getKey(), f);

			if (copyResult == null) {
				final String commanderError = commander.getLastErrorMessage();

				logger.log(Level.WARNING, "Could not download " + entry.getKey().getCanonicalName() + " to " + entry.getValue().getAbsolutePath() + ":\n" + commanderError);

				String traceLine = "ERROR: ";

				if (commanderError != null)
					traceLine += commanderError;
				else
					traceLine += "Could not download " + entry.getKey().getCanonicalName() + " to " + entry.getValue().getAbsolutePath();

				putJobTrace(traceLine);

				return false;
			}
		}

		logger.log(Level.INFO, "Sandbox populated: " + currentDir.getAbsolutePath());

		if (inputDataList != null) {
			// Dump inputDataList XML
			try {
				String collectionName = jdl.gets("InputDataList");

				if (collectionName == null || collectionName.isBlank())
					collectionName = "wn.xml";

				Files.write(Paths.get(currentDir.getAbsolutePath() + "/" + collectionName), inputDataList.getBytes());
			}
			catch (final Exception e) {
				logger.log(Level.SEVERE, "Problem dumping XML: ", e);
			}
		}

		return true;
	}

	private String createInputDataList() {
		logger.log(Level.INFO, "Starting XML creation");

		// creates xml file with the InputData
		try {
			final String list = jdl.gets("InputDataList");

			if (list == null) {
				logger.log(Level.WARNING, "XML List is NULL!");
				return null;
			}
			logger.log(Level.INFO, "Going to create: " + list);

			final String format = jdl.gets("InputDataListFormat");
			if (!"xml-single".equals(format) && !"txt-list".equals(format)) {
				logger.log(Level.WARNING, "Data list format not understood: " + format);
				return null;
			}

			final List<String> datalist = jdl.getInputData(true);

			if ("txt-list".equals(format)) {
				final StringBuilder sb = new StringBuilder();
				for (final String s : datalist)
					sb.append("alien://").append(s).append('\n');

				return sb.toString();
			}

			final XmlCollection c = new XmlCollection();
			c.setName("jobinputdata");

			// TODO: Change
			for (final String s : datalist) {
				final LFN l = c_api.getLFN(s);
				if (l == null)
					continue;
				c.add(l);
			}

			return c.toString();

			// logger.log(Level.WARNING, "Writing XML to:" + currentDir.getAbsolutePath() + "/" + list);
			// Files.write(Paths.get(currentDir.getAbsolutePath() + "/" + list), content.getBytes());

		}
		catch (final Exception e) {
			logger.log(Level.SEVERE, "Problem creating XML: ", e);
			return null;
		}
		// logger.log(Level.INFO, "XML creation has completed");;
	}

	private HashMap<String, String> getJobPackagesEnvironment() {
		final String voalice = "VO_ALICE@";
		final StringBuilder packages = new StringBuilder();
		final Map<String, String> packs = jdl.getPackages();
		HashMap<String, String> envmap = new HashMap<>();

		logger.log(Level.INFO, "Preparing to install packages");
		if (packs != null) {
			for (final Map.Entry<String, String> entry : packs.entrySet())
				packages.append(voalice + entry.getKey() + "::" + entry.getValue() + ",");

			if (!packs.containsKey("APISCONFIG"))
				packages.append(voalice + "APISCONFIG,");

			final String packagestring = packages.substring(0, packages.length() - 1);

			final ArrayList<String> packagesList = new ArrayList<>();
			packagesList.add(packagestring);

			logger.log(Level.INFO, packagestring);

			envmap = (HashMap<String, String>) installPackages(packagesList);
		}

		logger.log(Level.INFO, envmap.toString());
		return envmap;
	}

	private boolean uploadOutputFiles(final JobStatus exitStatus) {
		return uploadOutputFiles(exitStatus, 0);
	}

	private boolean uploadOutputFiles(final JobStatus exitStatus, final int exitCode) {
		boolean uploadedAllOutFiles = true;
		boolean uploadedNotAllCopies = false;

		logger.log(Level.INFO, "Uploading output for: " + jdl);

		final String outputDir = getJobOutputDir(exitStatus);

		putJobTrace("Going to uploadOutputFiles(exitStatus=" + exitStatus + ", outputDir=" + outputDir + ")");

		boolean noError = true;
		if (exitStatus.toString().contains("ERROR")) {
			putJobTrace("Registering temporary log files in " + outputDir + ". You must do 'registerOutput " + queueId
					+ "' within 24 hours of the job termination to preserve them. After this period, they are automatically deleted.");
			noError = false;
		}

		changeStatus(JobStatus.SAVING);

		logger.log(Level.INFO, "queueId: " + queueId);
		logger.log(Level.INFO, "resubmission: " + resubmission);
		logger.log(Level.INFO, "outputDir: " + outputDir);
		logger.log(Level.INFO, "We are the current user: " + commander.getUser().getName());

		final ArrayList<OutputEntry> archivesToUpload = new ArrayList<>();
		final ArrayList<OutputEntry> standaloneFilesToUpload = new ArrayList<>();
		final ArrayList<String> allArchiveEntries = new ArrayList<>();

		final ArrayList<String> outputTags = getOutputTags(exitStatus);
		for (final String tag : outputTags) {
			try {
				final ParsedOutput filesTable = new ParsedOutput(queueId, jdl, currentDir.getAbsolutePath(), tag, noError);
				for (final OutputEntry entry : filesTable.getEntries()) {
					if (entry.isArchive()) {
						logger.log(Level.INFO, "This is an archive: " + entry.getName());
						final ArrayList<String> archiveEntries = entry.createZip(currentDir.getAbsolutePath());
						if (archiveEntries.size() == 0) {
							logger.log(Level.WARNING, "Ignoring empty archive: " + entry.getName());
							putJobTrace("Ignoring empty archive: " + entry.getName());
						}
						else {
							for (final String archiveEntry : archiveEntries) {
								allArchiveEntries.add(archiveEntry);
								logger.log(Level.INFO, "Adding to archive members: " + archiveEntry);
							}
							archivesToUpload.add(entry);
						}
					}
					else {
						logger.log(Level.INFO, "This is not an archive: " + entry.getName());
						final File entryFile = new File(currentDir.getAbsolutePath() + "/" + entry.getName());
						if (entryFile.length() <= 0) { // archive files are checked for this during createZip, but standalone files still need to be checked
							logger.log(Level.WARNING, "The following file has size 0 and will be ignored: " + entry.getName());
							putJobTrace("The following file has size 0 and will be ignored: " + entry.getName());
						}
						else {
							standaloneFilesToUpload.add(entry);
							logger.log(Level.INFO, "Adding to standalone: " + entry.getName());
						}
					}
				}
			}
			catch (final NullPointerException ex) {
				logger.log(Level.SEVERE, "A required outputfile was NOT found! Aborting: " + ex.getMessage());
				putJobTrace("Error: A required outputfile was NOT found! Aborting: " + ex.getMessage());
				if (noError)
					changeStatus(JobStatus.ERROR_S);
				return false;
			}
		}

		final ArrayList<OutputEntry> toUpload = mergeAndRemoveDuplicateEntries(standaloneFilesToUpload, archivesToUpload, allArchiveEntries);
		for (final OutputEntry entry : toUpload) {
			try {
				final File localFile = new File(currentDir.getAbsolutePath() + "/" + entry.getName());
				logger.log(Level.INFO, "Processing output file: " + localFile);

				if (localFile.exists() && localFile.isFile() && localFile.canRead() && localFile.length() > 0) {
					putJobTrace("Uploading: " + entry.getName() + " to " + outputDir);

					final List<String> cpOptions = new ArrayList<>();
					cpOptions.add("-m");
					cpOptions.add("-S");

					if (entry.getOptions() != null && entry.getOptions().length() > 0)
						cpOptions.add(entry.getOptions());
					else
						cpOptions.add("disk:2");

					cpOptions.add("-j");
					cpOptions.add(String.valueOf(queueId));

					// Don't commit in case of ERROR_E or ERROR_V
					if (exitStatus == JobStatus.ERROR_E || exitStatus == JobStatus.ERROR_V)
						cpOptions.add("-nc");

					final ByteArrayOutputStream out = new ByteArrayOutputStream();
					final LFN uploadResult = IOUtils.upload(localFile, outputDir + "/" + entry.getName(), UserFactory.getByUsername(username), out, cpOptions.toArray(new String[0]));

					final String output_upload = out.toString();
					logger.log(Level.INFO,
							"Output result of " + localFile.getAbsolutePath() + " to " + outputDir + "/" + entry.getName() + " is:\nLFN = " + uploadResult + "\nFull `cp` output:\n"
									+ output_upload);

					if (uploadResult == null) {
						// complete failure to upload the file, mark the job as failed, not trying further to upload anything
						uploadedAllOutFiles = false;
						putJobTrace("Failed to upload to " + outputDir + "/" + entry.getName() + ": " + out.toString());
						break;
					}

					// success, but could all the copies requested in the JDL be created as per user specs?
					if (output_upload.contains("requested replicas could be uploaded")) {
						// partial success, will lead to a DONE_WARN state
						uploadedNotAllCopies = true;
						putJobTrace(output_upload);
					}
					else
						putJobTrace(uploadResult.getCanonicalName() + ": uploaded as requested");

					// archive entries are only booked when committed, so we have to do it ourselves since -nc
					if ((exitStatus == JobStatus.ERROR_E || exitStatus == JobStatus.ERROR_V) && entry.isArchive())
						CatalogueApiUtils.bookArchiveEntries(entry, uploadResult, outputDir + "/", UserFactory.getByUsername(username));
				}
				else {
					logger.log(Level.WARNING, "Can't upload output file " + localFile.getName() + ", does not exist or has zero size.");
					putJobTrace("Can't upload output file " + localFile.getName() + ", does not exist or has zero size.");
				}
			}
			catch (final IOException e) {
				logger.log(Level.WARNING, "IOException received while attempting to upload " + entry.getName(), e);
				putJobTrace("Failed to upload " + entry.getName() + " due to: " + e.getMessage());
				uploadedAllOutFiles = false;
			}
		}

		if (!uploadedAllOutFiles && exitStatus == JobStatus.DONE) {
			changeStatus(JobStatus.ERROR_SV);
			return false;
		} // else
			// changeStatus(JobStatus.SAVED); TODO: To be put back later if still needed
		if (exitStatus == JobStatus.DONE) {
			if (!registerEntries(toUpload, outputDir))
				changeStatus(JobStatus.ERROR_SV);
			else if (uploadedNotAllCopies)
				changeStatus(JobStatus.DONE_WARN);
			else
				changeStatus(JobStatus.DONE);
		}
		else
			changeStatus(exitStatus, exitCode);

		return uploadedAllOutFiles;
	}

	@SuppressWarnings("unchecked")
	private HashMap<String, String> loadJDLEnvironmentVariables() {
		final HashMap<String, String> hashret = new HashMap<>();

		try {
			final HashMap<String, Object> vars = (HashMap<String, Object>) jdl.getJDLVariables();

			if (vars != null)
				for (final String s : vars.keySet()) {
					String value = "";
					final Object val = jdl.get(s);

					if (val instanceof Collection<?>)
						value = String.join("##", (Collection<String>) val);
					else
						value = val.toString();

					hashret.put("ALIEN_JDL_" + s.toUpperCase(), value);
				}
		}
		catch (final Exception e) {
			logger.log(Level.WARNING, "There was a problem getting JDLVariables: " + e);
		}

		return hashret;
	}

	/**
	 * @param args
	 * @throws Exception
	 */
	public static void main(final String[] args) throws Exception {
		ConfigUtils.setApplicationName("JobWrapper");
		ConfigUtils.switchToForkProcessLaunching();

		final JobWrapper jw = new JobWrapper();
		jw.run();
	}

	/**
	 * Updates the current state of the job.
	 *
	 * @param newStatus
	 * @return <code>false</code> if the job was killed and execution should not continue
	 */
	public boolean changeStatus(final JobStatus newStatus) {
		return changeStatus(newStatus, 0);
	}

	/**
	 * Updates the current state of the job.
	 *
	 * @param newStatus
	 * @param exitCode
	 * @return <code>false</code> if the job was killed and execution should not continue
	 */
	public boolean changeStatus(final JobStatus newStatus, final int exitCode) {
		if (jobKilled)
			return false;

		jobStatus = newStatus;

		final HashMap<String, Object> extrafields = new HashMap<>();
		extrafields.put("exechost", ceHost);

		// if final status with saved files, we set the path
		if (jobStatus == JobStatus.DONE || jobStatus == JobStatus.DONE_WARN || jobStatus == JobStatus.ERROR_E || jobStatus == JobStatus.ERROR_V) {
			extrafields.put("path", getJobOutputDir(newStatus));
			if (exitCode != 0)
				extrafields.put("error", Integer.valueOf(exitCode));
		}
		else if (jobStatus == JobStatus.RUNNING) {
			extrafields.put("spyurl", hostName + ":" + TomcatServer.getPort());
			extrafields.put("node", hostName);
		}

		try {
			// Set the updated status
			if (!TaskQueueApiUtils.setJobStatus(queueId, resubmission, newStatus, extrafields)) {
				jobKilled = true;
				return false;
			}

			// TODO: Confirm(?) and remove
			// Wait 10s, and set status once more, in case the first attempt was not registered (high load?)
			// Thread.sleep(10 * 1000);
			// TaskQueueApiUtils.setJobStatus(queueId, newStatus, extrafields);

			// Also write status to file for the JobAgent to see
			Files.writeString(Paths.get(tmpDir + "/" + jobstatusFile), newStatus.name());
		}
		catch (final Exception e) {
			logger.log(Level.WARNING, "An error occurred when attempting to change current job status: " + e);
		}

		synchronized (statusSenderThread) {
			statusSenderThread.notifyAll();
		}

		return true;
	}

	/**
	 * @param exitStatus the target job status, affecting the booked directory (`~/recycle` if any error)
	 * @return job output dir (as indicated in the JDL if OK, or the recycle path if not)
	 */
	public String getJobOutputDir(final JobStatus exitStatus) {
		String outputDir = jdl.getOutputDir();

		if (exitStatus == JobStatus.ERROR_V || exitStatus == JobStatus.ERROR_E)
			outputDir = FileSystemUtils.getAbsolutePath(username, null, "~" + "recycle" + "/alien-job-" + queueId);
		else if (outputDir == null)
			outputDir = FileSystemUtils.getAbsolutePath(username, null, "~" + defaultOutputDirPrefix + queueId);

		return outputDir;
	}

	private void getTraceFromFile() {
		final File traceFile = new File(".alienValidation.trace");

		if (traceFile.exists() && traceFile.length() > 0) {
			try {
				final String trace = new String(Files.readAllBytes(traceFile.toPath()));

				if (!trace.isBlank())
					putJobTrace(trace);

				traceFile.delete();
			}
			catch (final Exception e) {
				logger.log(Level.WARNING, "An error occurred when reading .alienValidation.trace: " + e);
			}
		}

		logger.log(Level.INFO, ".alienValidation.trace does not exist.");
	}

	@Override
	public void fillValues(final Vector<String> paramNames, final Vector<Object> paramValues) {
		if (queueId > 0 && !jobKilled) {
			paramNames.add("jobID");
			paramValues.add(Double.valueOf(queueId));

			paramNames.add("statusID");
			paramValues.add(Double.valueOf(jobStatus.getAliEnLevel()));
		}
	}

	/**
	 * Cleanup processes, using a specialized script in CVMFS
	 *
	 * @param queueId AliEn job ID
	 * @param pid child process ID to start from
	 *
	 * @return script exit code, or -1 in case of error
	 */
	public static int cleanupProcesses(final long queueId, final int pid) {
		final File cleanupScript = new File(CVMFS.getCleanupScript());

		if (!cleanupScript.exists()) {
			logger.log(Level.WARNING, "Script for process cleanup not found in: " + cleanupScript.getAbsolutePath());
			return -1;
		}

		try {
			final ProcessBuilder pb = new ProcessBuilder(cleanupScript.getAbsolutePath(), "-v", "-m", "ALIEN_PROC_ID=" + queueId, String.valueOf(pid), "-KILL");
			pb.redirectError(Redirect.INHERIT);

			final Process process = pb.start();

			process.waitFor(30, TimeUnit.SECONDS);
			if (process.isAlive())
				process.destroyForcibly();

			try (InputStream is = process.getInputStream()) {
				final String result = new String(is.readAllBytes());
				logger.log(Level.INFO, result);
			}

			return process.exitValue();
		}
		catch (IOException | InterruptedException e) {
			logger.log(Level.WARNING, "An error occurred while attempting to run process cleanup: " + e);
			return -1;
		}
	}

	/**
	 * Register lfn links to archive
	 *
	 * @param entries
	 * @param outputDir
	 */
	private boolean registerEntries(final ArrayList<OutputEntry> entries, final String outputDir) {
		boolean registeredAll = true;
		for (final OutputEntry entry : entries) {
			try {
				final boolean registered = CatalogueApiUtils.registerEntry(entry, outputDir + "/", UserFactory.getByUsername(username));
				putJobTrace("Registering: " + entry.getName() + ". Return status: " + registered);

				if (!registered)
					registeredAll = false;
			}
			// TODO: Move up a layer, as this is not unique to registerEntry
			catch (final NullPointerException npe) {
				logger.log(Level.WARNING, "An error occurred while registering " + entry + ". Bad connection?", npe);
				putJobTrace("An error occurred while registering " + entry + ". Bad connection?");

				final int retries = 3;
				for (int i = 1; i <= retries; i++) {
					try {
						final boolean retrySuccess = CatalogueApiUtils.registerEntry(entry, outputDir + "/", UserFactory.getByUsername(username));
						if (retrySuccess) {
							logger.log(Level.INFO, "Entry " + entry + " successfully registered on attempt " + i);
							putJobTrace("Entry " + entry + " successfully registered on attempt " + i);
							break;
						}

						throw new NullPointerException("registerEntry returned `false`");
					}
					catch (final NullPointerException npe2) {
						logger.log(Level.WARNING, "Retry " + i + " failed.", npe2);
						if (i == 3) {
							logger.log(Level.SEVERE, "Registration of entry " + entry + " failed after 3 attempts. Aborting.");
							putJobTrace("Registration of entry " + entry + " failed after 3 attempts. Aborting.");
							return false;
						}

						try {
							Thread.sleep(30 * 1000);
						}
						catch (@SuppressWarnings("unused") final InterruptedException ie) {
							return false;
						}
					}
				}
			}
		}
		return registeredAll;
	}

	private ArrayList<String> getOutputTags(final JobStatus exitStatus) {
		final ArrayList<String> tags = new ArrayList<>();

		if (exitStatus == JobStatus.ERROR_E) {
			if (jdl.gets("OutputErrorE") == null) {
				putJobTrace("No output given for ERROR_E in JDL. Defaulting to std*");
				jdl.set("OutputErrorE", "log_archive.zip:std*@disk=1"); // set a default if nothing is provided
			}
			tags.add("OutputErrorE");
		}
		else {
			if (jdl.gets("OutputArchive") != null)
				tags.add("OutputArchive");
			if (jdl.gets("OutputFile") != null)
				tags.add("OutputFile");
			if (jdl.gets("Output") != null)
				tags.add("Output");
		}

		return tags;
	}

	private static ArrayList<OutputEntry> mergeAndRemoveDuplicateEntries(final ArrayList<OutputEntry> filesToMerge, final ArrayList<OutputEntry> fileList, final ArrayList<String> allArchiveEntries) {
		for (final OutputEntry file : filesToMerge) {
			if (!allArchiveEntries.contains(file.getName())) {
				logger.log(Level.INFO, "Standalone file not in any archive. To be uploaded separately: " + file.getName());
				fileList.add(file);
			}
		}
		return fileList;
	}

	private boolean jobKilled = false;

	private boolean putJobLog(final String key, final String value) {
		if (jobKilled)
			return false;

		if (!commander.q_api.putJobLog(queueId, resubmission, key, value)) {
			jobKilled = true;
			return false;
		}

		return true;
	}

	private boolean putJobTrace(final String value) {
		return putJobLog("trace", value);
	}
}
