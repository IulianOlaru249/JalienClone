package alien.api.catalogue;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import alien.api.Request;
import alien.catalogue.BookingTable;
import alien.catalogue.LFN;
import alien.config.ConfigUtils;
import alien.user.AliEnPrincipal;
import alien.user.UserFactory;
import lazyj.DBFunctions;

/**
 * Register the output of a (failed) job
 *
 * @author costing
 * @since 2019-07-15
 */
/**
 * @author costing
 *
 */
public class RegisterOutput extends Request {
	/**
	 * Logger
	 */
	static transient final Logger logger = ConfigUtils.getLogger(RegisterOutput.class.getCanonicalName());

	private static final long serialVersionUID = -2004904530203513524L;
	private final long jobID;

	private Collection<LFN> registeredLFNs = null;

	/**
	 * @param user
	 * @param jobID
	 */
	public RegisterOutput(final AliEnPrincipal user, final long jobID) {
		setRequestUser(user);
		this.jobID = jobID;
	}

	@Override
	public List<String> getArguments() {
		return Arrays.asList(String.valueOf(jobID));
	}

	@Override
	public void run() {
		final String jobOwner;

		try (DBFunctions db = ConfigUtils.getDB("processes")) {
			db.query("select user from QUEUE inner join QUEUE_USER using (userId) where queueId=?", false, Long.valueOf(jobID));

			if (!db.moveNext()) {
				logger.log(Level.WARNING, "Non existing job ID: " + jobID);
				return;
			}

			jobOwner = db.gets(1);
		}

		AliEnPrincipal requester = getEffectiveRequester();

		if (!requester.canBecome(jobOwner)) {
			logger.log(Level.WARNING, requester.getName() + " cannot register the output of " + jobID + " that belongs to " + jobOwner);
			return;
		}

		if (!requester.getName().equals(jobOwner)) {
			logger.log(Level.INFO, "Job " + jobID + " belonging to " + jobOwner + " is registered by " + requester.getName() + ", switching roles");
			requester = UserFactory.getByUsername(jobOwner);

			if (requester == null) {
				logger.log(Level.WARNING, "Could not instantiate a principal for username=" + jobOwner);
				return;
			}
		}

		registeredLFNs = BookingTable.registerOutput(requester, Long.valueOf(jobID));
	}

	/**
	 * @return the registered LFNs
	 */
	public Collection<LFN> getRegisteredLFNs() {
		return registeredLFNs;
	}
}
