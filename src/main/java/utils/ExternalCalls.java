package utils;

import java.io.File;

/**
 * @author ron
 * @since Oct 7, 2011
 */
public class ExternalCalls {

	/**
	 * @param program
	 * @return the full path to the program in env[PATH], or <code>null</code> if it could not be located anywhere
	 */
	public static String programExistsInPath(final String program) {
		return programExistsInFolders(program, System.getenv("PATH").split(":"));
	}

	/**
	 * Try to locate an executable in a collection of folders
	 * 
	 * @param program
	 *            executable to search for
	 * @param folders
	 *            paths to try
	 * @return the first executable found in the given folders
	 */
	public static String programExistsInFolders(final String program, final String... folders) {
		if (folders == null || folders.length == 0)
			return null;

		for (final String folder : folders) {
			final File dir = new File(folder);

			if (dir.exists() && dir.canRead()) {
				File test = new File(dir, program);

				if (test.exists() && test.isFile() && test.canExecute())
					return test.getAbsolutePath();
			}
		}

		return null;
	}
}
