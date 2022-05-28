/**
 *
 */
package utils;

import alien.taskQueue.TaskQueueUtils;
import lazyj.DBFunctions;
import lazyj.Format;

/**
 * @author costing
 * @since Jun 13, 2011
 */
public class RemoveSERequirements {

	/**
	 * Find waiting jobs having CloseSE requirements and remove the requirements so that they can run anywhere
	 *
	 * @param args
	 */
	public static void main(final String[] args) {

		if (args.length == 0)
			try (DBFunctions db = TaskQueueUtils.getQueueDB()) {
				db.setReadOnly(true);

				if (!db.query("SELECT queueId, jdl FROM QUEUE where status='WAITING' and jdl rlike '.*other.CloseSE.*';")) {
					System.err.println("Could not query the QUEUE, check your config/password.properies and config/processes.properties");
					return;
				}

				while (db.moveNext())
					cleanupRequirements(db.getl(1), db.gets(2));
			}
		else
			for (final String arg : args)
				try (DBFunctions db = TaskQueueUtils.getQueueDB()) {
					try {
						final long queueId = Long.parseLong(arg);

						if (!db.query("SELECT queueId, jdl FROM QUEUE where status='WAITING' and queueId=? AND jdl rlike '.*other.CloseSE.*';", false, Long.valueOf(queueId))) {
							System.err.println("Could not query the QUEUE, check your config/password.properies and config/processes.properties");
							return;
						}

						if (db.moveNext())
							cleanupRequirements(db.getl(1), db.gets(2));
					}
					catch (@SuppressWarnings("unused") final NumberFormatException nfe) {
						if (!db.query("SELECT queueId, jdl FROM QUEUE where status='WAITING' and jdl rlike '.*other.CloseSE.*" + Format.escSQL(arg) + ".*';")) {
							System.err.println("Could not query the QUEUE, check your config/password.properies and config/processes.properties");
							return;
						}

						if (db.moveNext())
							cleanupRequirements(db.getl(1), db.gets(2));
					}
				}
	}

	/**
	 * @param geti
	 * @param gets
	 */
	private static void cleanupRequirements(final long queueId, final String jdl) {
		final int idx = jdl.indexOf(" Requirements = ");

		if (idx < 0) {
			System.err.println(queueId + " : could not locate Requirements");
			return;
		}

		String newJDL = jdl.substring(0, idx);

		int idx2 = jdl.indexOf('\n', idx);

		if (idx2 < 0)
			idx2 = jdl.length();

		String requirements = jdl.substring(idx, idx2);

		System.err.println(queueId + " : old requirements : " + requirements);

		requirements = requirements.replaceAll("&& \\( member\\(other.CloseSE,\\\".+::.+::.+\\\"\\)( || member\\(other.CloseSE,\\\".+::.+::.+\\\"\\))* \\)", "");

		System.err.println(queueId + " : new requirements : " + requirements);

		newJDL += requirements;

		if (idx2 < jdl.length())
			newJDL += jdl.substring(idx2);

		try (DBFunctions db = TaskQueueUtils.getQueueDB()) {
			final boolean ok = db.query("UPDATE QUEUE SET jdl=? WHERE queueId=? AND status='WAITING'", false, newJDL, Long.valueOf(queueId));
			// final boolean ok = false;

			if (ok && db.getUpdateCount() == 1)
				System.err.println(queueId + " : queue updated successfully");
			else
				System.err.println(queueId + " : failed to update the waiting job : queue ok=" + ok + ", update count=" + db.getUpdateCount());
		}
	}

}
