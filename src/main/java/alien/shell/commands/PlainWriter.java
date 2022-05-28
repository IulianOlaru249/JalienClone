package alien.shell.commands;

import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import alien.config.ConfigUtils;
import lazyj.Format;

/**
 * @author ron
 * @since July 15, 2011
 */
public class PlainWriter extends UIPrintWriter {

	/**
	 * Logger
	 */
	static final Logger logger = ConfigUtils.getLogger(PlainWriter.class.getCanonicalName());

	/**
	 *
	 */
	public static final String lineTerm = "\n";
	/**
	 *
	 */
	public static final String SpaceSep = " ";

	/**
	 * error String tag to mark a println for stderr
	 */
	public static final String errTag = "ERR: ";

	/**
	 * String tag to mark the last line of an output
	 */
	public static String outputterminator = "\n";

	/**
	 * String tag to mark the last line of an transaction stream
	 */
	public static final String streamend = String.valueOf((char) 0);

	/**
	 * String tag to mark separated fields
	 */
	public static String fieldseparator = String.valueOf((char) 1);

	/**
	 * String tag to signal pending action
	 */
	public static String pendingSignal = String.valueOf((char) 9);

	/**
	 * marker for -Colour argument
	 */
	protected boolean bColour = true;

	/**
	 * Metainfo of the command
	 */
	private HashMap<String, String> metadataResult;

	@Override
	protected void blackwhitemode() {
		bColour = false;
	}

	@Override
	protected void colourmode() {
		bColour = true;
	}

	/**
	 * color status
	 *
	 * @return state of the color mode
	 */
	@Override
	protected boolean colour() {
		return bColour;
	}

	private final OutputStream os;

	/**
	 * @param os
	 */
	public PlainWriter(final OutputStream os) {
		this.os = os;
		metadataResult = new HashMap<>();
	}

	private void print(final String line) {
		try {
			os.write(line.getBytes());
			os.flush();
		}
		catch (final IOException e) {
			e.printStackTrace();
			logger.log(Level.FINE, "Could not write to OutputStream" + line, e);
		}
	}

	private static String cleanupLine(final String line) {
		if (line.indexOf('\0') < 0)
			return line;

		return Format.replace(line, "\0", "");
	}

	@Override
	protected void printOut(final String line) {
		print(cleanupLine(line));
	}

	@Override
	protected void printErr(final String line) {
		final String cleanedUpLine = cleanupLine(line);

		print(errTag + cleanedUpLine);
		setMetaInfo("error", cleanedUpLine);
	}

	@Override
	protected void setenv(final String cDir, final String user) {
		// ignore
	}

	@Override
	protected void flush() {
		print(streamend);
	}

	@Override
	protected void pending() {
		// ignore
	}

	@Override
	protected void nextResult() {
		// ignored
	}

	@Override
	public void setField(final String key, final Object value) {
		// ignored
	}

	@Override
	public void setMetaInfo(final String key, final String value) {
		if (key != null && !key.isEmpty())
			if (value != null)
				metadataResult.put(key, value);
			else
				metadataResult.remove(key);
	}

	@Override
	public void setReturnCode(final int exitCode, final String errorMessage) {
		if (!errorMessage.isEmpty()) {
			printErr(errorMessage);
		}
		setMetaInfo("exitcode", String.valueOf(exitCode));
		setMetaInfo("error", errorMessage);
	}

	@Override
	public String getMetaInfo(String key) {
		return key != null && !key.isEmpty() ? metadataResult.get(key).toString() : "";
	}

	@Override
	public int getReturnCode() {
		return Integer.parseInt(getMetaInfo("exitcode"));
	}

	@Override
	public String getErrorMessage() {
		return getMetaInfo("error");
	}
}
