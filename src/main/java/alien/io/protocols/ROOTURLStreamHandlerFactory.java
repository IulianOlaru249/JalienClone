package alien.io.protocols;

import java.net.URLStreamHandler;
import java.net.URLStreamHandlerFactory;

/**
 * A new URL handler factory that knows about root:// protocols
 *
 * @author costing
 * @since 2016-07-18
 */
public class ROOTURLStreamHandlerFactory implements URLStreamHandlerFactory {

	@Override
	public URLStreamHandler createURLStreamHandler(final String protocol) {
		if ("root".equals(protocol))
			return new ROOTURLStreamHandler();

		return null;
	}

}
