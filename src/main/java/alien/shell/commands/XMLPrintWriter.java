package alien.shell.commands;

import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;

import alien.config.ConfigUtils;

/**
 * @author costing
 * @since Mar 27 2014
 */
public class XMLPrintWriter extends UIPrintWriter {
	/**
	 * Logger
	 */
	static final Logger logger = ConfigUtils.getLogger(XMLPrintWriter.class.getCanonicalName());

	private XMLStreamWriter writer;

	/**
	 * @param os
	 *            OutputSteam that will contain the information in Root format
	 */
	public XMLPrintWriter(final OutputStream os) {
		final XMLOutputFactory factory = XMLOutputFactory.newInstance();

		try {
			writer = factory.createXMLStreamWriter(new PrintWriter(os));
		}
		catch (@SuppressWarnings("unused") final XMLStreamException xmlEx) {
			// ignore
		}

		setReturnCode(0, null);
	}

	private final Map<String, String> metaInfo = new TreeMap<>();

	private final List<Map<String, Object>> results = new ArrayList<>();

	private Map<String, Object> currentResult = null;

	@Override
	protected void blackwhitemode() {
		// nothing
	}

	@Override
	protected void colourmode() {
		// nothing
	}

	@Override
	protected boolean colour() {
		return false;
	}

	@Override
	protected void printOut(final String line) {
		// nothing
	}

	@Override
	protected void printErr(final String line) {
		// nothing
	}

	@Override
	protected void setenv(final String cDir, final String user) {
		metaInfo.put("pwd", cDir);
		metaInfo.put("user", user);
	}

	@Override
	protected void flush() {
		nextResult();

		metaInfo.put("count", String.valueOf(results.size()));

		try {
			writer.writeStartElement("document");

			for (final Map.Entry<String, String> entry : metaInfo.entrySet())
				writer.writeAttribute(entry.getKey(), entry.getValue());

			for (final Map<String, Object> result : results) {
				writer.writeStartElement("r");

				for (final Map.Entry<String, Object> entry : result.entrySet()) {
					writer.writeStartElement(entry.getKey());

					final Object o = entry.getValue();

					if (o instanceof String) {
						String value = (String) o;

						if (value.indexOf(' ') >= 0 || value.indexOf('\n') >= 0 || value.indexOf('\r') >= 0)
							writer.writeCData(value);
						else
							writer.writeCharacters(value);
					}
					else {
						if (o instanceof Map) {
							@SuppressWarnings("unchecked")
							final Map<Object, Object> values = (Map<Object, Object>) o;

							for (final Map.Entry<Object, Object> e2 : values.entrySet()) {
								writer.writeStartElement("r2");
								writer.writeAttribute("k", e2.getValue().toString());
								writer.writeAttribute("v", e2.getValue().toString());
								writer.writeEndElement();
							}
						}
					}

					writer.writeEndElement();
				}

				writer.writeEndElement();
			}

			writer.writeEndElement();

			writer.flush();
		}
		catch (final XMLStreamException xmlEx) {
			logger.log(Level.WARNING, "Exception writing XML to the client", xmlEx);
		}

		results.clear();
		metaInfo.clear();

		setReturnCode(0, null);
	}

	@Override
	protected void pending() {
		// nothing
	}

	@Override
	protected boolean isRootPrinter() {
		return true;
	}

	@Override
	protected void nextResult() {
		if (currentResult != null) {
			results.add(currentResult);
			currentResult = null;
		}
	}

	@Override
	public void setField(final String key, final Object value) {
		if (currentResult == null)
			currentResult = new TreeMap<>();

		currentResult.put(key, value);
	}

	/**
	 * Set a result meta information
	 *
	 * @param key
	 * @param value
	 */
	@Override
	public void setMetaInfo(final String key, final String value) {
		if (value != null)
			metaInfo.put(key, value);

		metaInfo.remove(key);
	}

	@Override
	public void setReturnCode(final int exitCode, final String errorMessage) {
		setMetaInfo("exitcode", String.valueOf(exitCode));
		setMetaInfo("message", errorMessage);
	}

	@Override
	public String getMetaInfo(String key) {
		return "";
	}

	@Override
	public int getReturnCode() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public String getErrorMessage() {
		// TODO Auto-generated method stub
		return "";
	}
}
