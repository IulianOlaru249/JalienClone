package alien.shell;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import jline.console.completer.FileNameCompleter;

/**
 * @author ron
 * @since September 29, 2011
 */
public class GridLocalFileCompletor extends FileNameCompleter {

	private final BusyBox busy;

	/**
	 * @param box
	 */
	public GridLocalFileCompletor(final BusyBox box) {
		busy = box;
	}

	@SuppressWarnings("unchecked")
	@Override
	public int complete(final String buf, final int cursor, @SuppressWarnings("rawtypes") final List candidates) {
		if (buf != null && buf.contains("file://") && cursor >= buf.indexOf("file://"))
			return super.complete(buf.replace("file://", ""), cursor - 7, candidates) + 7;

		return gridComplete(buf, candidates);
	}

	private int gridComplete(final String buf, final List<String> candidates) {

		final String buffer = (buf == null) ? "" : buf;

		String translated = buffer;

		// special character: ~ maps to the user's home directory
		if (translated.startsWith("~" + File.separator))
			translated = busy.getCurrentDir() + translated.substring(2);
		else if (translated.startsWith("~"))
			translated = busy.getCurrentDir();
		else if (!(translated.startsWith(File.separator)))
			translated = busy.getCurrentDir() // + File.separator
					+ translated;

		final String dir;

		if (translated.endsWith(File.separator))
			dir = translated;
		else
			dir = translated.substring(0, translated.lastIndexOf('/'));

		final String listing = busy.callJBoxGetString("ls -ca " + dir);

		final StringTokenizer tk = new StringTokenizer(listing);
		final List<String> entries = new ArrayList<>();
		while (tk.hasMoreElements())
			entries.add((String) tk.nextElement());

		try {
			return gridMatchFiles(buffer, translated, entries, candidates);

		}
		finally {
			// we want to output a sorted list of files
			// sortFileNames(candidates);
		}
	}

	private static int gridMatchFiles(final String buffer, final String translated, final List<String> entries, final List<String> candidates) {

		if (entries == null)
			return -1;

		int matches = 0;

		// first pass: just count the matches
		for (final String lfn : entries)
			if (lfn.startsWith(translated))
				matches++;

		for (String lfn : entries)
			if (lfn.startsWith(translated)) {
				String name = null;
				if (lfn.endsWith("/")) {
					lfn = lfn.substring(0, lfn.length() - 1);
					name = lfn.substring(lfn.lastIndexOf('/') + 1) + ((matches == 1) ? "/" : "");
				}
				else
					name = lfn.substring(lfn.lastIndexOf('/') + 1) + ((matches == 1) ? " " : "");

				candidates.add(name);
			}

		final int index = buffer.lastIndexOf("/");

		return index + 1;
	}
}
