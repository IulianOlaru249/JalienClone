package alien.test.utils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

import lia.util.process.ExternalProcess.ExitStatus;
import lia.util.process.ExternalProcessBuilder;

/**
 * @author ron
 * @since Sep 16, 2011
 */
public class TestCommand {

	private String stderr = "";

	private String stdout = "";

	private boolean verbose = false;

	private boolean daemonize = false;

	private ArrayList<String> command = new ArrayList<>();

	/**
	 * 
	 */
	public TestCommand() {
		// nothing
	}

	/**
	 * @param command
	 */
	public TestCommand(ArrayList<String> command) {
		for (String c : command)
			this.command.add(c.trim());
	}

	/**
	 * @param command
	 */
	public TestCommand(String[] command) {
		for (String c : command)
			this.command.add(c.trim());
	}

	/**
	 * @param command
	 */
	public TestCommand(String command) {
		for (String c : command.split(" "))
			this.command.add(c);
	}

	/**
	 * @param comm
	 */
	public void addCommand(String comm) {
		command.add(comm.trim());
	}

	/**
	 * @return command
	 */
	public String getCommand() {
		final StringBuilder out = new StringBuilder();

		for (final String c : command) {
			if (out.length() > 0)
				out.append(' ');

			out.append(c);
		}

		return out.toString();
	}

	/**
	 * @return stdout
	 */
	public String getStdOut() {
		return stdout;
	}

	/**
	 * @return stderr
	 */
	public String getStdErr() {
		return stderr;
	}

	/**
	 * set verbose
	 */
	public void verbose() {
		verbose = true;
	}

	/**
	 * set daemonize
	 */
	public void daemonize() {
		daemonize = true;
	}

	/**
	 * @return success of the shell execution
	 */
	public boolean exec() {

		if (verbose)
			System.out.println("EXEC: " + getCommand());

		final ExternalProcessBuilder pBuilder = new ExternalProcessBuilder(new ArrayList<>(command));

		if (!daemonize)
			pBuilder.returnOutputOnExit(true);

		// pBuilder.directory();

		pBuilder.timeout(30, TimeUnit.SECONDS);

		if (!daemonize)
			pBuilder.redirectErrorStream(true);

		try {
			final ExitStatus exitStatus;

			if (daemonize) {
				pBuilder.start();
				return true;
			}

			exitStatus = pBuilder.start().waitFor();

			stdout = exitStatus.getStdOut().trim();
			stderr = exitStatus.getStdErr().trim();
			if (exitStatus.getExtProcExitStatus() != 0) {

				System.out.println("Error while executing [" + getCommand() + "]...");
				System.out.println("STDOUT: " + stdout);
				System.out.println("STDERR: " + stderr);
				return false;
			}

		}
		catch (final InterruptedException ie) {
			System.err.println("Interrupted while waiting for the following command to finish : " + command.toString() + " : " + ie.getMessage());
			return false;
		}
		catch (IOException e) {
			System.err.println("IOException while waiting for the following command to finish : " + command.toString() + " : " + e.getMessage());
			return false;
		}
		return true;

	}
}
