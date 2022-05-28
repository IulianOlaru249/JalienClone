package alien;

import alien.config.ConfigUtils;
import alien.site.JobAgent;

/**
 * @author mmmartin, ron
 * @since Apr 1, 2015
 */
public class StartJobAgent {

	/**
	 * @param args
	 * @throws Exception
	 */
	public static void main(final String[] args) throws Exception {
		ConfigUtils.switchToForkProcessLaunching();

		// try {
		// JAKeyStore.loadPilotKeyStorage();
		// } catch (Exception e) {
		// e.printStackTrace();
		// }
		//
		// DispatchSSLClient.overWriteServiceAndForward("siteProxyService");

		final JobAgent jA = new JobAgent();
		jA.run();
	}
}
