package alien.config;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import lazyj.ExtProperties;
import lazyj.FallbackProperties;

/**
 * @author nhardi
 *
 *         ConfigManager can handle multiple configuration sources and resolve their priorities.
 *         Each configuration source has to implement ConfigSource interface and has to be registered
 *         with methods registerPrimary() or registerFallback().
 *
 *         There are two levels of configuration keys, the configuration file and configuration key
 *         inside of a configuration file. Keys need not to be unique across files. All keys defined in
 *         configuration files with the same name are unified by ConfigManager.
 *
 *         A configuration files is an abstract collection of configuration keys and doesn't have to be
 *         a physical file. The keys can be put together into a configuration file from LDAP, database or
 *         any other source implemented by ConfigSource.
 *
 *         To get unified view of all registered configuration sources, use method getConfiguration().
 *
 *         ConfigManager preserves configuration auto-reloading where applicable.
 *
 */
public class ConfigManager implements ConfigSource {
	private Map<String, ExtProperties> cfgStorage;

	/**
	 * Create a ConfigManager instance without any registered sources.
	 */
	public ConfigManager() {
		cfgStorage = new HashMap<>();
	}

	/**
	 * Register a configuration source with the highest priority, overwriting values
	 * coming from any previously registered sources. Effectively, insert this source
	 * into the front of a list of all sources. The list is read from the front.
	 *
	 * @param cfgSource
	 *            The new source to be registered.
	 */
	public void registerPrimary(ConfigSource cfgSource) {
		registerSource(cfgSource, true);
	}

	/**
	 * Register a configuration source with the lowest priority that will be used only
	 * if a key has not been found in any of previously registered sources. Effectively,
	 * insert this source into the end of a list of all sources. The list is read from the front.
	 *
	 * @param cfgSource
	 *            The new source to be registered.
	 */
	public void registerFallback(ConfigSource cfgSource) {
		registerSource(cfgSource, false);
	}

	private void registerSource(ConfigSource cfgSource, boolean overwrite) {
		Map<String, ExtProperties> newConfiguration = cfgSource.getConfiguration();

		for (final Map.Entry<String, ExtProperties> entry : newConfiguration.entrySet()) {
			String name = entry.getKey();
			ExtProperties oldProp = cfgStorage.get(name);
			ExtProperties newProp = entry.getValue();

			ExtProperties merged = mergeProperties(oldProp, newProp, overwrite);
			cfgStorage.put(name, merged);
		}
	}

	@Override
	public Map<String, ExtProperties> getConfiguration() {
		return cfgStorage;
	}

	/**
	 * Make configuration coming from all registered configuration sources read-only.
	 * This will not prevent auto-reloading configuration by changing a file on file-system.
	 */
	public void makeReadonly() {
		for (final Map.Entry<String, ExtProperties> entry : cfgStorage.entrySet()) {
			final ExtProperties prop = entry.getValue();
			prop.makeReadOnly();
		}

		cfgStorage = Collections.unmodifiableMap(cfgStorage);
	}

	/**
	 * Create a new ExtProperties object that merges the two ExtProperties objects provided
	 * as parameters. If both inputs are null, then an empty ExtProperties object is created.
	 *
	 * If overwrite is set to false, then parameter a has precedence.
	 * If overwrite is set to true, then parameter b has precedence.
	 *
	 * This method relies on lazyj.FallbackProperties.
	 *
	 * @param a old (existing) configuration
	 * @param b new configuration
	 * @param overwrite true if the new configuration should have precedence
	 * @return ExtProperties or FallbackProperties when multiple properties are added.
	 */
	public static ExtProperties mergeProperties(final ExtProperties a, final ExtProperties b, final boolean overwrite) {
		if (a == null && b == null) {
			return new ExtProperties();
		}
		else if (a != null && b == null) {
			return a;
		}
		else if (a == null && b != null) {
			return b;
		}

		FallbackProperties tmp;
		if (a instanceof FallbackProperties) {
			tmp = (FallbackProperties) a;
			tmp.addProvider(b, overwrite);
		}
		else {
			tmp = new FallbackProperties();
			tmp.addProvider(a);
			tmp.addProvider(b, overwrite);
		}
		return tmp;
	}

	/**
	 * Create merged ExtProperties, and old configuration (parameter a) has precedence.
	 * 
	 * @see #mergeProperties(ExtProperties, ExtProperties, boolean)
	 *
	 * @param a old (existing) configuration
	 * @param b new configuration
	 * @return ExtProperties or FallbackProperties when multiple properties are added.
	 */
	public static ExtProperties mergeProperties(final ExtProperties a, final ExtProperties b) {
		return mergeProperties(a, b, false);
	}
}
