package alien.io.protocols;

import java.io.IOException;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLStreamHandler;

/**
 * Handler for root:// protocols
 *
 * @author costing
 * @since 2016-07-18
 */
public class ROOTURLStreamHandler extends URLStreamHandler {

	@Override
	protected URLConnection openConnection(final URL u) throws IOException {
		// TODO If ever we can open the content of a root file, say how here
		return null;
	}

}
