package alien.shell.commands;

import java.io.IOException;
import java.io.OutputStream;
import java.util.LinkedHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import alien.config.ConfigUtils;

/**
 * @author vyurchen
 *
 */
public class JSONPrintWriter extends UIPrintWriter {

	/**
	 * Logger
	 */
	static final Logger logger = ConfigUtils.getLogger(JSONPrintWriter.class.getCanonicalName());

	private final OutputStream os;

	private final JSONArray resultArray;
	private final JSONObject metadataResult;

	private LinkedHashMap<String, Object> currentResult;

	/**
	 * @param os
	 */
	public JSONPrintWriter(final OutputStream os) {
		this.os = os;
		resultArray = new JSONArray();
		metadataResult = new JSONObject();
	}

	@Override
	protected void blackwhitemode() {
		// ignore
	}

	@Override
	protected void colourmode() {
		// ignore
	}

	@Override
	protected boolean colour() {
		// ignore
		return false;
	}

	@Override
	protected void printOut(final String line) {
		if (currentResult == null)
			currentResult = new LinkedHashMap<>();

		// If there already was a message, concatenate strings
		if (currentResult.get("message") != null)
			currentResult.put("message", currentResult.get("message") + line);
		else
			currentResult.put("message", line);
	}

	@Override
	protected void printErr(final String line) {
		setMetaInfo("error", line);
	}

	@Override
	protected void setenv(final String cDir, final String user) {
		setMetaInfo("user", user);
		setMetaInfo("currentdir", cDir);
	}

	/**
	 * Write data to the client OutputStream
	 */
	@SuppressWarnings("unchecked")
	@Override
	protected void flush() {
		nextResult(); // Add the last item and go

		try {
			final JSONObject replyObject = new JSONObject();
			if (metadataResult.size() > 0)
				replyObject.put("metadata", metadataResult);

			replyObject.put("results", resultArray);
			os.write(replyObject.toJSONString().getBytes());
			os.flush();
		}
		catch (final IOException e) {
			e.printStackTrace();
			logger.log(Level.FINE, "Could not write JSON to the client OutputStream", e);
		}

		resultArray.clear();
		metadataResult.clear();
	}

	@Override
	protected void pending() {
		// ignore
	}

	@SuppressWarnings("unchecked")
	@Override
	protected void nextResult() {
		if (currentResult != null) {
			// The ROOT client doesn't expect a newline character at the end of a JSON string, remove it

			if (currentResult.get("message") != null)
				currentResult.put("message", currentResult.get("message").toString().replaceAll("\n+$", ""));

			resultArray.add(currentResult);
			currentResult = null;
		}
	}

	@Override
	public void setField(final String key, final Object value) {
		if (currentResult == null)
			currentResult = new LinkedHashMap<>();

		currentResult.put(key, value);
	}

	/**
	 * Set a result meta information
	 *
	 * @param key
	 * @param value
	 */
	@SuppressWarnings("unchecked")
	@Override
	public void setMetaInfo(final String key, final String value) {
		if (key != null && !key.isEmpty())
			if (value != null)
				metadataResult.put(key, value);
			else
				metadataResult.remove(key);
	}

	/**
	 * Get a value from metainfo
	 *
	 * @param key the field you are interested in
	 */
	@Override
	public String getMetaInfo(final String key) {
		return key != null && !key.isEmpty() ? metadataResult.get(key).toString() : "";
	}

	@Override
	public void setReturnCode(final int exitCode, final String errorMessage) {
		setMetaInfo("exitcode", String.valueOf(exitCode));
		setMetaInfo("error", errorMessage);
	}

	@Override
	public int getReturnCode() {
		return Integer.parseInt(getMetaInfo("exitcode"));
	}

	@Override
	public String getErrorMessage() {
		return getMetaInfo("error");
	}

	@Override
	protected boolean isRootPrinter() {
		return true;
	}
}
