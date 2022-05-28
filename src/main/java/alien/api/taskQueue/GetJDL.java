package alien.api.taskQueue;

import java.util.Arrays;
import java.util.List;

import alien.api.Request;
import alien.taskQueue.Job;
import alien.taskQueue.TaskQueueUtils;
import alien.user.AliEnPrincipal;

/**
 * Get a JDL object
 *
 * @author ron
 * @since Oct 26, 2011
 */
public class GetJDL extends Request {
	/**
	 *
	 */
	private static final long serialVersionUID = 5445861914172537974L;

	private final long queueId;

	private String jdl;

	private boolean originalJDL = true;

	/**
	 * @param user
	 * @param queueId job ID
	 * @param originalJDL whether to return the original JDL (<code>true</code>) or the resulting JDL (<code>false</code>)
	 */
	public GetJDL(final AliEnPrincipal user, final long queueId, final boolean originalJDL) {
		setRequestUser(user);
		this.queueId = queueId;
		this.originalJDL = originalJDL;
	}

	@Override
	public List<String> getArguments() {
		return Arrays.asList(String.valueOf(this.queueId), String.valueOf(this.originalJDL));
	}

	@Override
	public void run() {
		this.jdl = Job.sanitizeJDL(TaskQueueUtils.getJDL(queueId, originalJDL));
	}

	/**
	 * @return a JDL
	 */
	public String getJDL() {
		return this.jdl;
	}

	@Override
	public String toString() {
		return "Asked for JDL :  reply is: " + this.jdl;
	}
}
