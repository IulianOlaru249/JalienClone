/**
 * 
 */
package utils;

/**
 * @author costing
 * @since Nov 6, 2011
 */
public class SystemProcess {

	/**
	 * @param cmd
	 * @return the exit code
	 */
	private native int systemCall(String cmd);

	private final String command;

	/**
	 * Prepare to run some system command
	 * 
	 * @param command
	 */
	public SystemProcess(final String command) {
		this.command = command;
	}

	/**
	 * @return the exit code
	 */
	public int execute() {
		return systemCall(command);
	}

	static {
		System.loadLibrary("SystemProcess");
	}
}
