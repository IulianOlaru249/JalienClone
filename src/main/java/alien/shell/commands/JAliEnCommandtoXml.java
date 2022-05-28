package alien.shell.commands;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import alien.catalogue.FileSystemUtils;
import alien.catalogue.LFN;
import alien.catalogue.XmlCollection;
import alien.io.IOUtils;
import alien.shell.ErrNo;
import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;

/**
 * @author ron
 * @since June 4, 2011
 * @author sraje (Shikhar Raje, IIIT Hyderabad)
 * @since Modified 27th July, 2012
 */
public class JAliEnCommandtoXml extends JAliEnBaseCommand {

	private String targetXml = null;
	private Set<String> fileLists = new LinkedHashSet<>();
	private Set<String> lfns = new LinkedHashSet<>();

	private boolean ignoreMissingFiles = false;
	private boolean append = false;

	@Override
	public void run() {
		final String username = commander.user.getName();
		final String cwd = commander.getCurrentDirName();

		for (final String listEntry : fileLists) {
			String expandedListPath = FileSystemUtils.getAbsolutePath(username, cwd, listEntry);

			File localListFile = catFile(expandedListPath);

			if (localListFile == null) {
				commander.setReturnCode(ErrNo.EIO, "Could not get the contents of " + listEntry);
				return;
			}

			try (BufferedReader br = new BufferedReader(new FileReader(localListFile))) {
				String line;

				while ((line = br.readLine()) != null) {
					line = line.trim();

					lfns.add(FileSystemUtils.getAbsolutePath(username, cwd, line));
				}
			}
			catch (@SuppressWarnings("unused") Exception e) {
				commander.setReturnCode(ErrNo.EIO, "Cannot parse content of " + listEntry);
			}

			localListFile.delete();
		}

		if (lfns.size() == 0) {
			commander.setReturnCode(ErrNo.ENODATA, "Empty content");
			return;
		}

		XmlCollection collection;

		if (append) {
			try {
				File localXmlFile = catFile(targetXml);

				if (localXmlFile == null) {
					commander.setReturnCode(ErrNo.EIO, "Could not load the content of " + targetXml + " to append to");
					return;
				}

				collection = new XmlCollection(localXmlFile);
				localXmlFile.delete();
			}
			catch (@SuppressWarnings("unused") final IOException ioe) {
				commander.setReturnCode(ErrNo.EIO, "Cannot read the content of " + targetXml);
				return;
			}
		}
		else
			collection = new XmlCollection();

		List<LFN> resolvedLFNs = commander.c_api.getLFNs(lfns, true, false);

		if (resolvedLFNs.size() != lfns.size()) {
			if (!ignoreMissingFiles) {
				commander.setReturnCode(ErrNo.ENOENT, "Only " + resolvedLFNs.size() + " out of " + lfns.size() + " files could be resolved");
				return;
			}

			commander.printErrln("Only " + resolvedLFNs.size() + " out of " + lfns.size() + " files could be resolved");
		}

		if (resolvedLFNs.size() == 0) {
			commander.setReturnCode(ErrNo.ENODATA, "Empty collection");
			return;
		}

		int oldSize = collection.size();

		collection.addAll(resolvedLFNs);

		if (oldSize == collection.size()) {
			commander.printOut("Collection was not modified, all " + resolvedLFNs.size() + " entries were already included in it");
			return;
		}

		collection.setOwner(username);
		collection.setCommand(String.join(" ", alArguments));
		collection.setName(targetXml);

		if (targetXml == null)
			commander.printOutln(collection.toString());
		else {
			// try to upload the collection to the target location

			targetXml = FileSystemUtils.getAbsolutePath(username, cwd, targetXml);

			if (append) {
				commander.c_api.removeLFN(targetXml + "~");
				commander.c_api.moveLFN(targetXml, targetXml + "~");
			}

			try {
				// Create a local temp file
				final File f = File.createTempFile("collection-" + System.currentTimeMillis(), ".xml");

				if (f != null) {
					// Save xml collection to local file
					final String content = collection.toString();
					try (BufferedWriter o = new BufferedWriter(new FileWriter(f))) {
						o.write(content);
					}

					// Upload this file to grid
					IOUtils.upload(f, targetXml, commander.getUser(), 4, null, true);
				}
			}
			catch (final Exception e) {
				commander.setReturnCode(ErrNo.EIO, "Upload failed: " + e.getMessage());
			}
		}
	}

	/**
	 * printout the help info
	 */
	@Override
	public void printHelp() {
		commander.printOutln();
		commander.printOutln(helpUsage("toXml", "[-i] [-x xml_file_name] [-a] [-l list_from] [lfns]"));
		commander.printOutln(helpStartOptions());
		commander.printOutln(helpOption("-i", "ignore missing entries, continue even if some paths are not/no longer available"));
		commander.printOutln(helpOption("-x", "write the XML content directly in this target AliEn file"));
		commander.printOutln(helpOption("-a", "(requires -x) append to the respective collection"));
		commander.printOutln(helpOption("-l", "read the list of LFNs from this file, one path per line"));
	}

	/**
	 * cd can run without arguments
	 *
	 * @return <code>true</code>
	 */
	@Override
	public boolean canRunWithoutArguments() {
		return false;
	}

	/**
	 * @return the arguments as a String array
	 */
	public String[] getArgs() {
		return alArguments.size() > 1 ? alArguments.subList(1, alArguments.size()).toArray(new String[0]) : null;
	}

	/**
	 * @param fileName
	 *            catalogue file name to cat
	 * @return file handle for downloaded file
	 */
	public File catFile(final String fileName) {
		final ArrayList<String> args = new ArrayList<>(2);
		args.add("-t");
		args.add(fileName);

		JAliEnCommandcp cp;
		try {
			cp = (JAliEnCommandcp) JAliEnCOMMander.getCommand("cp", new Object[] { commander, args });
		}
		catch (final Exception e) {
			e.printStackTrace();
			return null;
		}

		silent();

		try {
			final Thread backgroundCP = new Thread(cp);
			backgroundCP.start();
			while (backgroundCP.isAlive()) {
				Thread.sleep(500);
				commander.pending();
			}
		}
		catch (final Exception e) {
			e.printStackTrace();
			return null;
		}

		verbose();

		return cp.getOutputFile();
	}

	/**
	 * Constructor needed for the command factory in commander
	 *
	 * @param commander
	 *
	 * @param alArguments
	 *            the arguments of the command
	 */
	public JAliEnCommandtoXml(final JAliEnCOMMander commander, final List<String> alArguments) {
		super(commander, alArguments);
		try {
			final OptionParser parser = new OptionParser();

			parser.accepts("i");
			parser.accepts("l").withRequiredArg();
			parser.accepts("x").withRequiredArg();
			parser.accepts("a");

			final OptionSet options = parser.parse(alArguments.toArray(new String[] {}));

			ignoreMissingFiles = options.has("i");

			if (options.has("x"))
				targetXml = FileSystemUtils.getAbsolutePath(commander.getUsername(), commander.getCurrentDirName(), options.valueOf("x").toString());

			if (options.has("a")) {
				append = true;

				if (targetXml == null) {
					commander.printErrln("You have to pass -x as well if you want to append to an existing collection");
					setArgumentsOk(false);
				}
			}

			if (options.has("l"))
				for (final Object o : options.valuesOf("l"))
					fileLists.add(FileSystemUtils.getAbsolutePath(commander.getUsername(), commander.getCurrentDirName(), o.toString()));

			for (Object o : options.nonOptionArguments())
				lfns.add(FileSystemUtils.getAbsolutePath(commander.getUsername(), commander.getCurrentDirName(), o.toString()));
		}
		catch (final OptionException | IllegalArgumentException e) {
			commander.setReturnCode(ErrNo.EINVAL, e.getMessage());
			setArgumentsOk(false);
		}
	}
}
