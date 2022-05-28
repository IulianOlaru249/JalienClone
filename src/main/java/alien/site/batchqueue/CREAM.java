package alien.site.batchqueue;

import java.sql.Timestamp;
import java.util.HashMap;
import java.util.logging.Logger;

import alien.log.LogUtils;

/**
 * @author mmmartin
 */
public class CREAM extends BatchQueue {

	// private boolean updateClassad = false;
	private HashMap<String, String> commands = new HashMap<>();
	// private Map<String, String> environment = System.getenv();

	/**
	 * @param conf
	 * @param logr
	 *            logger
	 */
	public CREAM(HashMap<String, Object> conf, Logger logr) {
		this.config = conf;
		logger = logr;
		logger = LogUtils.redirectToCustomHandler(logger, ((String) config.get("host_logdir")) + "JAliEn." + (new Timestamp(System.currentTimeMillis()).getTime() + ".out"));

		logger.info("This VO-Box is " + config.get("ALIEN_CM_AS_LDAP_PROXY") + ", site is " + config.get("site_accountname"));

		// Environment for submit

		// String fix_env = "";
		// if (environment.containsKey("LD_LIBRARY_PATH")) {
		// fix_env += environment.get("LD_LIBRARY_PATH");
		// if (environment.containsKey("JALIEN_ROOT")) {
		// fix_env = fix_env.replaceAll(environment.get("JALIEN_ROOT") + "+[^:]+:?", "");
		// fix_env = "unset X509_CERT_DIR; LD_LIBRARY_PATH=" + fix_env;
		// }
		// }

		// Commands
		commands.put("submitcmd", (config.containsKey("ce_submitcmd") ? (String) config.get("ce_submitcmd") : "glite-ce-job-submit")); // need fix_env?
		commands.put("statuscmd", (config.containsKey("ce_statuscmd") ? (String) config.get("ce_statuscmd") : "glite-ce-job-status"));
		commands.put("killcmd", (config.containsKey("ce_killcmd") ? (String) config.get("ce_killcmd") : "glite-ce-job-cancel"));
		commands.put("delegationcmd", (config.containsKey("ce_delegationcmd") ? (String) config.get("ce_delegationcmd") : "glite-ce-delegate-proxy"));
	}

	@Override
	public void submit(final String script) {
		logger.info("Submit CREAM");
	}

	@Override
	public int getNumberActive() {
		return 0;
	}

	@Override
	public int getNumberQueued() {
		return 0;
	}

	@Override
	public int kill() {
		return 0;
	}

}
