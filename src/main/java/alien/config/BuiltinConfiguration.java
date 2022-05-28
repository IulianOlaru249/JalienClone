package alien.config;

import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

import lazyj.ExtProperties;

/**
 * @author nhardi
 *
 */
public class BuiltinConfiguration implements ConfigSource {
	// TODO: break dependency on ConfigUtils!
	@Override
	public Map<String, ExtProperties> getConfiguration() {
		Map<String, ExtProperties> tmpProperties = new HashMap<>();

		try {
			for (String name : ConfigUtils.getResourceListing(ConfigUtils.class, "config/"))
				if (name.endsWith(".properties")) {
					if (name.indexOf('/') > 0)
						name = name.substring(name.lastIndexOf('/') + 1);

					final String key = name.substring(0, name.indexOf('.'));

					try (InputStream is = ConfigUtils.class.getClassLoader().getResourceAsStream("config/" + name)) {
						final ExtProperties prop = new ExtProperties(is);
						tmpProperties.put(key, prop);
					}
				}
		}
		catch (@SuppressWarnings("unused") final Throwable t) {
			// cannot load the default configuration files for any reason
		}

		return tmpProperties;
	}
}
