/**
 * 
 */
package alien.api.taskQueue;

import java.util.Arrays;
import java.util.List;

import alien.api.Request;
import alien.config.ConfigUtils;
import lazyj.DBFunctions;

/**
 * @author costing
 * @since Mar 31, 2020
 */
public class FaHTask extends Request {

	/**
	 * Generated serial version UID
	 */
	private static final long serialVersionUID = 5265156551246089957L;

	/**
	 * Input - job ID that will be assigned a workdir
	 */
	private final long jobId;

	private int sequenceId = -1;

	/**
	 * @param jobId current job ID to which the slot should be assigned to
	 */
	public FaHTask(final long jobId) {
		this.jobId = jobId;
	}

	@Override
	public void run() {
		try (DBFunctions db = ConfigUtils.getDB("processes"); DBFunctions db2 = ConfigUtils.getDB("processes")) {
			db.setReadOnly(true);

			final String username = getEffectiveRequester().getName();

			// all jobs that are in a final state are free to take
			db.query("select F.fah_uid,F.queueId from FAH_WORKDIR F left outer join QUEUE Q using(queueId) WHERE username=? AND (statusId is null or statusId<6 OR statusId>12);", false, username);

			while (db.moveNext()) {
				final int fah_uid = db.geti(1);
				final long oldJobID = db.getl(2);

				db2.query("update FAH_WORKDIR set queueId=" + jobId + " WHERE fah_uid=" + fah_uid + " AND queueId=" + oldJobID);

				if (db2.getUpdateCount() > 0) {
					// found a slot that was not taken yet, was booked for us, return it
					sequenceId = fah_uid;
					return;
				}
			}

			db2.setLastGeneratedKey(true);

			// no slot was available, we have to insert a new one
			if (db2.query("insert into FAH_WORKDIR (username, queueId) VALUES (?, ?);", false, username, Long.valueOf(jobId)))
				sequenceId = db2.getLastGeneratedKey().intValue();
		}
	}

	/**
	 * @return a strictly positive number that is the slot ID where the jobs should read/write data to, or <code>-1</code> if the operation failed for any reason
	 */
	public int getSequenceId() {
		return sequenceId;
	}

	@Override
	public List<String> getArguments() {
		return Arrays.asList(String.valueOf(jobId));
	}
}
