package alien.api.taskQueue;

import java.util.Arrays;
import java.util.List;
import java.util.Map.Entry;

import alien.api.Request;
import alien.taskQueue.TaskQueueUtils;
import alien.user.AliEnPrincipal;

/**
 * Resubmit a job
 *
 * @author mmmartin
 * @since Nov 22, 2017
 */
public class ResubmitJob extends Request {

	private static final long serialVersionUID = -3089514055558736684L;

	private final long queueId;

	private Entry<Integer, String> resubmitEntry;

	/**
	 * @param user
	 * @param queueId
	 */
	public ResubmitJob(final AliEnPrincipal user, final long queueId) {
		setRequestUser(user);
		this.queueId = queueId;
	}

	@Override
	public List<String> getArguments() {
		return Arrays.asList(String.valueOf(queueId));
	}

	@Override
	public void run() {
		this.resubmitEntry = TaskQueueUtils.resubmitJob(getEffectiveRequester(), queueId);
	}

	/**
	 * @return success of the resubmit
	 */
	public Entry<Integer, String> resubmitEntry() {
		return this.resubmitEntry;
	}

	@Override
	public String toString() {
		return "Asked to resubmit job: " + this.queueId;
	}

}
