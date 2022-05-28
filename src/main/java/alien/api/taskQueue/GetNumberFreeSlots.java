package alien.api.taskQueue;

import java.util.Arrays;
import java.util.List;

import alien.api.Request;
import alien.taskQueue.JobBroker;
import alien.user.AliEnPrincipal;

/**
 * Get number of slots for a CE
 *
 * @author mmmartin
 * @since Feb 20, 2017
 */
public class GetNumberFreeSlots extends Request {
	/**
	 *
	 */
	private static final long serialVersionUID = 5445861914172987654L;

	private List<Integer> slots;
	private final String ceName;
	private final String host;
	private final int port;
	private final String version;

	/**
	 * @param user
	 * @param host
	 * @param port
	 * @param ce
	 * @param version
	 */
	public GetNumberFreeSlots(final AliEnPrincipal user, final String host, final int port, final String ce, final String version) {
		setRequestUser(user);
		this.host = host;
		this.port = port;
		this.ceName = ce;
		this.version = version;
	}

	@Override
	public List<String> getArguments() {
		return Arrays.asList(host, String.valueOf(port), ceName, version);
	}

	@Override
	public void run() {
		this.slots = JobBroker.getNumberFreeSlots(host, port, ceName, version);
	}

	/**
	 * @return code and job slots
	 */
	public List<Integer> getJobSlots() {
		return this.slots;
	}

	@Override
	public String toString() {
		return "Asked for number of free slots for host: " + host + " CE: " + ceName + ": " + this.slots.toString();
	}
}
