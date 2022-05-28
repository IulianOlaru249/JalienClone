package alien.config;

import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import lazyj.ExtProperties;

class ConfigManagerTests {
	private class MockConfigSource implements ConfigSource {
		private Map<String, ExtProperties> cfgStorage;

		public MockConfigSource() {
			cfgStorage = new HashMap<>();
		}

		public void set(String file, String key, String value) {
			if (!cfgStorage.containsKey(file)) {
				cfgStorage.put(file, new ExtProperties());
			}

			cfgStorage.get(file).set(key, value);
		}

		@Override
		public Map<String, ExtProperties> getConfiguration() {
			return cfgStorage;
		}
	}

	static void assertKey(ExtProperties merged, String key, String expected) {
		String read = merged.gets(key);
		Assertions.assertEquals(expected, read);
	}

	static void assertKey(ConfigManager mgr, String config, String key, String expected) {
		assertKey(mgr.getConfiguration().get(config), key, expected);
	}

	@Test
	void testSingleSource() {
		MockConfigSource src = new MockConfigSource();
		ConfigManager cfgManager = new ConfigManager();

		src.set("config", "key", "a");
		cfgManager.registerPrimary(src);
		assertKey(cfgManager, "config", "key", "a");
	}

	@Test
	static void testMergeProperties() {
		ExtProperties front = new ExtProperties();
		ExtProperties back = new ExtProperties();
		ExtProperties merged;

		front.set("key", "a");
		back.set("key", "b");

		merged = ConfigManager.mergeProperties(front, back);
		assertKey(merged, "key", "a");

		merged = ConfigManager.mergeProperties(back, front);
		assertKey(merged, "key", "b");
	}

	@Test
	static void testMergePropertiesUpdate() {
		ExtProperties front = new ExtProperties();
		ExtProperties back = new ExtProperties();
		ExtProperties merged;

		front.set("otherkey", "c");
		back.set("key", "b");
		merged = ConfigManager.mergeProperties(front, back);

		front.set("key", "a");
		assertKey(merged, "key", "a");
	}

	@Test
	void testReloadingConfigurationsSingleSource() {
		MockConfigSource src = new MockConfigSource();
		ConfigManager cfgManager = new ConfigManager();

		src.set("config", "key", "a");
		cfgManager.registerPrimary(src);

		assertKey(cfgManager, "config", "key", "a");

		src.set("config", "key", "b");
		assertKey(cfgManager, "config", "key", "b");
	}

	@Test
	void testReloadConfigurationsMultipleSources() {
		MockConfigSource srcA = new MockConfigSource();
		MockConfigSource srcB = new MockConfigSource();
		ConfigManager cfgManager = new ConfigManager();

		srcA.set("config", "otherkey", "a");
		srcB.set("config", "key", "b");

		cfgManager.registerPrimary(srcA);
		cfgManager.registerFallback(srcB);
		assertKey(cfgManager, "config", "key", "b");

		srcA.set("config", "key", "a");
		assertKey(cfgManager, "config", "key", "a");
	}

	@Test
	void testReloadConfigurationPrimary() {
		MockConfigSource srcA = new MockConfigSource();
		MockConfigSource srcB = new MockConfigSource();
		ConfigManager cfgManager = new ConfigManager();

		srcA.set("config", "otherkey", "x");
		srcB.set("config", "otherkey", "x");
		cfgManager.registerPrimary(srcA);
		cfgManager.registerPrimary(srcB);

		assertKey(cfgManager, "config", "key", "");

		srcA.set("config", "key", "a");
		assertKey(cfgManager, "config", "key", "a");

		srcB.set("config", "key", "b");
		assertKey(cfgManager, "config", "key", "b");
	}

	@Test
	void testReloadConfigurationFallback() {
		MockConfigSource srcA = new MockConfigSource();
		MockConfigSource srcB = new MockConfigSource();
		ConfigManager cfgManager = new ConfigManager();

		srcA.set("config", "otherkey", "x");
		srcB.set("config", "otherkey", "x");
		cfgManager.registerPrimary(srcA);
		cfgManager.registerFallback(srcB);

		assertKey(cfgManager, "config", "key", "");

		srcA.set("config", "key", "a");
		assertKey(cfgManager, "config", "key", "a");

		srcB.set("config", "key", "b");
		assertKey(cfgManager, "config", "key", "a");
	}
}
