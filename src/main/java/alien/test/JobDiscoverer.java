package alien.test;

import java.lang.reflect.Field;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;

/**
 *
 */
public class JobDiscoverer {
	private final static Field callableInFutureTask;
	@SuppressWarnings("rawtypes")
	private static final Class<? extends Callable> adapterClass;
	private static final Field runnableInAdapter;

	static {
		try {
			callableInFutureTask = FutureTask.class.getDeclaredField("callable");
			callableInFutureTask.setAccessible(true);
			adapterClass = Executors.callable(new Runnable() {
				@Override
				public void run() {
					// nothing
				}
			}).getClass();
			runnableInAdapter = adapterClass.getDeclaredField("task");
			runnableInAdapter.setAccessible(true);
		}
		catch (final NoSuchFieldException e) {
			throw new ExceptionInInitializerError(e);
		}
	}

	/**
	 * @param task
	 * @return the task
	 */
	public static Object findRealTask(final Runnable task) {
		if (task instanceof FutureTask)
			try {
				final Object callable = callableInFutureTask.get(task);
				if (adapterClass.isInstance(callable))
					return runnableInAdapter.get(callable);
				return callable;
			}
			catch (final IllegalAccessException e) {
				throw new IllegalStateException(e);
			}
		throw new ClassCastException("Not a FutureTask");
	}
}