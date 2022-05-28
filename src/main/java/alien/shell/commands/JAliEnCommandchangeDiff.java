package alien.shell.commands;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import alien.io.protocols.TempFileManager;
import alien.shell.ErrNo;
import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import lazyj.commands.CommandOutput;
import lazyj.commands.SystemCommand;

/**
 * @author ron
 * @since June 4, 2011
 */
public class JAliEnCommandchangeDiff extends JAliEnBaseCommand {
	private ArrayList<String> alPaths = null;

	@Override
	public void run() {
		for (final String eachFileName : alPaths) {
			final File fout = catFile(eachFileName);

			if (fout == null) {
				commander.setReturnCode(ErrNo.ENOENT, eachFileName);
				return;
			}

			final File fold = catFile(eachFileName + "~");

			if (fold == null) {
				commander.setReturnCode(ErrNo.ENOENT, eachFileName + "~");
				TempFileManager.release(fout);
				return;
			}

			try {
				final CommandOutput co = SystemCommand.executeCommand(Arrays.asList("diff", fold.getAbsolutePath(), fout.getAbsolutePath()));
				commander.printOutln(co.stdout);
			}
			finally {
				TempFileManager.release(fout);
				TempFileManager.release(fold);
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
		commander.printOutln("Show changes between the current version of the file and the previous one (same file name with a '~' suffix)");
		commander.printOutln(helpUsage("changeDiff", "[<filename>]"));
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
	public JAliEnCommandchangeDiff(final JAliEnCOMMander commander, final List<String> alArguments) throws OptionException {
		super(commander, alArguments);

		try {
			final OptionParser parser = new OptionParser();

			final OptionSet options = parser.parse(alArguments.toArray(new String[] {}));

			alPaths = new ArrayList<>(options.nonOptionArguments().size());
			alPaths.addAll(optionToString(options.nonOptionArguments()));
		}
		catch (final OptionException e) {
			commander.setReturnCode(ErrNo.EINVAL, e.getMessage());
			setArgumentsOk(false);
		}
	}

	@Override
	public String toString() {
		final StringBuilder sb = new StringBuilder();

		sb.append("\n { JAliEnCommandchangeDiff\n");
		sb.append("Arguments: ");
		sb.append("}");

		return sb.toString();
	}
}
