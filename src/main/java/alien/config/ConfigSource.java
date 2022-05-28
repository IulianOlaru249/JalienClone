package alien.config;

import java.util.Map;

import lazyj.ExtProperties;

/**
 * @author nhardi
 *
 *         Get a collection of properties files from arbitrary source.
 * @see ConfigManager
 *
 */
public interface ConfigSource {
	/**
	 * @return collection of properties files from this source, with their names
	 */
	public Map<String, ExtProperties> getConfiguration();
}
