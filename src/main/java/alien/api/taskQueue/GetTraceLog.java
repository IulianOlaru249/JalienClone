package alien.api.taskQueue;

import java.util.Arrays;
import java.util.List;

import alien.api.Request;
import alien.taskQueue.TaskQueueUtils;
import alien.user.AliEnPrincipal;

/**
 * Get a TraceLog object
 *
 * @author ron
 * @since Oct 26, 2011
 */
public class GetTraceLog extends Request {
	/**
	 *
	 */
	private static final long serialVersionUID = 5022083696413315512L;

	private String trace = "";

	private final long queueId;

	/**
	 * @param user
	 * @param queueId
	 */
	public GetTraceLog(final AliEnPrincipal user, final long queueId) {
		setRequestUser(user);
		this.queueId = queueId;
	}

	@Override
	public List<String> getArguments() {
		return Arrays.asList(String.valueOf(queueId));
	}

	@Override
	public void run() {
		trace = TaskQueueUtils.getJobTraceLog(queueId);
	}

	/**
	 * @return a JDL
	 */
	public String getTraceLog() {
		return this.trace;
	}

	@Override
	public String toString() {
		return "Asked for TraceLog :  reply is: " + this.trace;
	}
}
