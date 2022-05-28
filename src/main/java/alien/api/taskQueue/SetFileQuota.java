package alien.api.taskQueue;

import java.util.Arrays;
import java.util.List;

import alien.api.Request;
import alien.quotas.QuotaUtilities;
import alien.user.AliEnPrincipal;

/**
 * Set the file quota for a given account
 *
 * @author costing
 *
 */
public class SetFileQuota extends Request {

	private static final long serialVersionUID = 1286883117531333434L;
	private boolean succeeded;
	private final String field;
	private final String value;
	private final String username;

	/**
	 * @param user
	 * @param fld
	 * @param val
	 */
	public SetFileQuota(final AliEnPrincipal user, final String fld, final String val) {
		this.field = fld;
		this.value = val;
		this.username = user.getName();
	}

	@Override
	public List<String> getArguments() {
		return Arrays.asList(username, field, value);
	}

	@Override
	public void run() {
		if (!getEffectiveRequester().canBecome("admin"))
			throw new SecurityException("Only administrators can do it");

		this.succeeded = QuotaUtilities.saveFileQuota(this.username, this.field, this.value);
	}

	/**
	 * @return <code>true</code> if the operation was successful
	 */
	public boolean getSucceeded() {
		return this.succeeded;
	}
}
