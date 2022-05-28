package alien.config;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author costing
 */
public class Context {

	/**
	 * Thread-tied contexts
	 */
	static Map<Thread, Map<String, Object>> context = new ConcurrentHashMap<>();

	static {
		final Thread cleanupThread = new Thread("alien.config.Context.cleanup") {
			@Override
			public void run() {
				while (true)
					try {
						Thread.sleep(1000 * 60);

						context.keySet().retainAll(Thread.getAllStackTraces().keySet());
					}
					catch (@SuppressWarnings("unused") final Throwable t) {
						// ignore
					}
			}
		};

		cleanupThread.setDaemon(true);
		cleanupThread.start();
	}

	/**
	 * Associate an object to a key in current thread's context
	 * 
	 * @param key
	 * @param value
	 * @return the previously set value for this key
	 * 
	 * @see #resetContext()
	 */
	public static Object setThreadContext(final String key, final Object value) {
		Map<String, Object> m = context.get(Thread.currentThread());

		if (m == null) {
			// the map will be accessed from within the same thread, so there
			// can be no conflict here
			m = new HashMap<>();
			context.put(Thread.currentThread(), m);
		}

		return m.put(key, value);
	}

	/**
	 * Get the content of a particular key from the current thread's context
	 * 
	 * @param key
	 * @return the value associated with this key for the current thread. Can be <code>null</code> if the key is not set
	 */
	public static Object getTheadContext(final String key) {
		final Map<String, Object> m = context.get(Thread.currentThread());

		if (m == null)
			return null;

		return m.get(key);
	}

	/**
	 * Get the entire current thread's context map
	 * 
	 * @return everything that is set in current thread's context. Can be <code>null</code>
	 */
	public static Map<String, Object> getThreadContext() {
		return context.get(Thread.currentThread());
	}

	/**
	 * Reset current thread's context so that previously set values don't leak into next call's environment
	 * 
	 * @return previous content, if any
	 */
	public static Map<String, Object> resetContext() {
		return context.remove(Thread.currentThread());
	}
}
