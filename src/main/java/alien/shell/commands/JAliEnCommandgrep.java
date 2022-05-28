package alien.shell.commands;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

import alien.catalogue.FileSystemUtils;
import alien.catalogue.LFN;
import alien.io.protocols.TempFileManager;
import alien.shell.ErrNo;
import joptsimple.OptionException;
import lazyj.commands.CommandOutput;
import lazyj.commands.SystemCommand;

/**
 * @author costing
 * @since 2019-08-30
 */
public class JAliEnCommandgrep extends JAliEnBaseCommand {

	private final ArrayList<String> flags = new ArrayList<>();

	private String pattern = null;

	private final ArrayList<String> alPaths = new ArrayList<>();

	/**
	 * Print the file name for each match. This is the default when there is more than one file to search.
	 */
	private boolean bH = false;

	@Override
	public void run() {
		final LFN currentDir = commander.getCurrentDir();

		final List<String> filesToProcess = new ArrayList<>();

		for (final String path : alPaths) {
			final String absolutePath = FileSystemUtils.getAbsolutePath(commander.user.getName(), currentDir != null ? currentDir.getCanonicalName() : null, path);

			final List<String> expandedPaths = FileSystemUtils.expandPathWildCards(absolutePath, commander.user);

			if (expandedPaths != null && !expandedPaths.isEmpty())
				filesToProcess.addAll(expandedPaths);
			else
				commander.setReturnCode(ErrNo.ENOENT, path);
		}

		if (filesToProcess.size() > 1)
			bH = true;

		if (flags.remove("-h"))
			bH = false;

		if (flags.remove("-H"))
			bH = true;

		for (final String eachFileName : filesToProcess) {
			final File fout = catFile(eachFileName);

			try {
				if (fout != null && fout.exists() && fout.isFile() && fout.canRead()) {
					final ArrayList<String> commandToExecute = new ArrayList<>();
					commandToExecute.add("grep");

					if (flags.size() > 0)
						commandToExecute.addAll(flags);

					commandToExecute.add(pattern);
					commandToExecute.add(fout.getAbsolutePath());

					final CommandOutput co = SystemCommand.executeCommand(commandToExecute, false);

					if (!co.stderr.isEmpty()) {
						if (bH)
							commander.printErr(eachFileName + ": ");

						commander.printErrln(co.stderr);
					}

					if (!co.stdout.isEmpty()) {
						if (bH) {
							try (BufferedReader br = new BufferedReader(new StringReader(co.stdout))) {
								String line = null;
								while ((line = br.readLine()) != null)
									commander.printOutln(eachFileName + ": " + line);
							}
							catch (@SuppressWarnings("unused") final IOException e) {
								// ignore, will not happen when reading from Strings
							}
						}
						else {
							commander.printOutln(co.stdout);
						}
					}
				}
				else {
					commander.setReturnCode(ErrNo.EREMOTEIO, "Not able to get the file: " + eachFileName);
				}
			}
			finally {
				TempFileManager.release(fout);
			}
		}
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
	 * printout the help info
	 */
	@Override
	public void printHelp() {
		commander.printOutln();
		commander.printOutln(helpUsage("grep", "[-linux grep options] <pattern> [<filename>]+"));
		commander.printOutln(helpStartOptions());
		commander.printOutln();
	}

	/**
	 * cat cannot run without arguments
	 *
	 * @return <code>false</code>
	 */
	@Override
	public boolean canRunWithoutArguments() {
		return false;
	}

	/**
	 * Constructor needed for the command factory in JAliEnCOMMander
	 *
	 * @param commander
	 *
	 * @param alArguments
	 *            the arguments of the command
	 * @throws OptionException
	 */
	public JAliEnCommandgrep(final JAliEnCOMMander commander, final List<String> alArguments) throws OptionException {
		super(commander, alArguments);

		try {
			boolean option = true;

			for (final String s : alArguments) {
				if (option) {
					if (!s.startsWith("-")) {
						option = false;

						if (pattern == null)
							pattern = s;
						else
							alPaths.add(s);
					}
					else {
						flags.add(s);

						if (s.equals("-e"))
							option = false;
					}
				}
				else {
					if (pattern == null)
						pattern = s;
					else
						alPaths.add(s);
				}
			}

			if (pattern == null) {
				commander.printErrln("grep requires a pattern to search for, and at least one file to look at");
				setArgumentsOk(false);
			}
			else if (alPaths.size() == 0) {
				commander.printErrln("grep requires some files to process");
				setArgumentsOk(false);
			}
		}
		catch (final OptionException e) {
			commander.setReturnCode(ErrNo.EINVAL, e.getMessage());
			setArgumentsOk(false);
		}
	}

	@Override
	public String toString() {
		final StringBuilder sb = new StringBuilder();

		sb.append("\n { JAliEnCommandgrepreceived\n");
		sb.append("Arguments: ");

		sb.append(flags.toString());
		sb.append(pattern);
		sb.append(alPaths.toString());

		sb.append("}");

		return sb.toString();
	}
}
