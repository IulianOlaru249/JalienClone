/**
 *
 */
package alien.user;

import java.util.Set;
import java.util.logging.Logger;

import alien.catalogue.CatalogEntity;
import alien.config.ConfigUtils;
import alien.taskQueue.Job;

/**
 * @author costing
 * @since Nov 11, 2010
 */
public final class AuthorizationChecker {

	/**
	 * Logger
	 */
	static final Logger logger = ConfigUtils.getLogger(AuthorizationChecker.class.getCanonicalName());

	private AuthorizationChecker() {
		// utility class
	}

	/**
	 * Check if the user owns this entity
	 *
	 * @param entity
	 * @param user
	 * @return true if the user owns this entity
	 */
	public static boolean isOwner(final CatalogEntity entity, final AliEnPrincipal user) {
		if (user == null || entity == null)
			return false;
		return user.canBecome(entity.getOwner());
	}

	/**
	 * Check if the user is in the same group as the owner of this file
	 *
	 * @param entity
	 * @param user
	 * @return true if the user is in the same group
	 */
	public static boolean isGroupOwner(final CatalogEntity entity, final AliEnPrincipal user) {
		if (user == null || entity == null)
			return false;
		return user.hasRole(entity.getGroup());
	}

	/**
	 * Get the permission field that applies to the user
	 *
	 * @param entity
	 * @param user
	 * @return permission field
	 */
	public static int getPermissions(final CatalogEntity entity, final AliEnPrincipal user) {

		if (user == null || entity == null)
			return 0;

		final Set<String> accounts = user.getNames();

		if (accounts != null && accounts.contains("admin"))
			return 7;

		if (user.hasRole("admin"))
			return 7;

		if (isOwner(entity, user))
			return entity.getPermissions().charAt(0) - '0';

		if (isGroupOwner(entity, user))
			return entity.getPermissions().charAt(1) - '0';

		return entity.getPermissions().charAt(2) - '0';
	}

	/**
	 * Check if the user can read the entity
	 *
	 * @param entity
	 * @param user
	 * @return true if the user can read it
	 */
	public static boolean canRead(final CatalogEntity entity, final AliEnPrincipal user) {
		if (user == null || entity == null)
			return false;
		if ((getPermissions(entity, user) & 4) == 4) {
			logger.fine("The user \"" + user.getName() + "\" has the right to read \"" + entity.getName() + "\"");
			return true;
		}

		logger.fine("The user \"" + user.getName() + "\" has no right to read \"" + entity.getName() + "\"");
		return false;

	}

	/**
	 * Check if the user can write the entity
	 *
	 * @param entity
	 * @param user
	 * @return true if the user can write it
	 */
	public static boolean canWrite(final CatalogEntity entity, final AliEnPrincipal user) {
		if (user == null || entity == null)
			return false;
		if ((getPermissions(entity, user) & 2) == 2) {
			logger.fine("The user \"" + user.getName() + "\" has the right to write \"" + entity.getName() + "\"");
			return true;
		}

		logger.fine("The user \"" + user.getName() + "\" has no right to write \"" + entity.getName() + "\"");
		return false;

	}

	/**
	 * Check if the user can execute the entity
	 *
	 * @param entity
	 * @param user
	 * @return true if the user can execute it
	 */
	public static boolean canExecute(final CatalogEntity entity, final AliEnPrincipal user) {
		if (user == null || entity == null)
			return false;
		return (getPermissions(entity, user) & 1) == 1;
	}

	/**
	 * Check if the user can modify the job
	 *
	 * @param job
	 *
	 * @param user
	 * @return true if the user can execute it
	 */
	public static boolean canModifyJob(final Job job, final AliEnPrincipal user) {
		if (job == null || user == null)
			return false;

		if (job.getOwner().equals(user.getName()) || user.hasRole(job.getOwner()) || user.hasRole("admin"))
			return true;

		return false;
	}

}
