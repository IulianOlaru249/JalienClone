package alien.api;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import alien.config.ConfigUtils;

/**
 * @author costing
 * @since 2019-06-07
 */
public class Ping extends Request {

	/**
	 * Generated serial UID
	 */
	private static final long serialVersionUID = -2970460898632648056L;

	private Map<String, String> serverInfo = null;

	@Override
	public void run() {
		serverInfo = new LinkedHashMap<>();

		serverInfo.put("hostname", ConfigUtils.getLocalHostname());
	}

	/**
	 * Server side returns some information on its status.
	 *
	 * @return some server provided information
	 */
	public Map<String, String> getServerInfo() {
		return serverInfo;
	}

	@Override
	public List<String> getArguments() {
		return null;
	}
}
