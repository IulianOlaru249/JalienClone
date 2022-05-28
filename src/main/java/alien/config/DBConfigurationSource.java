package alien.config;

import java.util.HashMap;
import java.util.Map;

import lazyj.DBFunctions;
import lazyj.DBProperties;
import lazyj.ExtProperties;

/**
 * Extra configuration can be loaded directly from the database, on central services
 */
class DBConfigurationSource implements ConfigSource {
	private Map<String, ExtProperties> oldConfig;
	private boolean isCentralService;

	/**
	 * @param oldConfig
	 * @param isCentralService
	 */
	public DBConfigurationSource(final Map<String, ExtProperties> oldConfig, boolean isCentralService) {
		this.oldConfig = oldConfig;
		this.isCentralService = isCentralService;
	}

	@Override
	public Map<String, ExtProperties> getConfiguration() {
		Map<String, ExtProperties> dbConfig = new HashMap<>();
		dbConfig.put("config", getConfigFromDB(oldConfig.get("config")));
		return dbConfig;
	}

	private ExtProperties getConfigFromDB(final ExtProperties fileConfig) {
		ExtProperties tmp = new ExtProperties();

		if (isCentralService && fileConfig.getb("jalien.config.hasDBBackend", true)) {
			try (final DBFunctions dbAdmin = new DBFunctions(oldConfig.get("admin"))) {
				final DBProperties dbProp = new DBProperties(dbAdmin);
				dbProp.makeReadOnly();
				tmp = dbProp;
			}
		}

		return tmp;
	}
}
