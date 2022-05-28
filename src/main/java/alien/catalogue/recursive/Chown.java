package alien.catalogue.recursive;

import alien.catalogue.LFN_CSD;
import alien.user.AuthorizationChecker;

/**
 * @author mmmartin
 *
 */
public class Chown extends RecursiveOp {

	@Override
	public boolean callback(LFN_CSD l) {
		// check permissions to chown
		if (!AuthorizationChecker.isOwner(l, user)) {
			lfns_error.add(l);
			return false;
		}
		// chown and add to the final collection of lfns
		if (!l.owner.equals(new_owner) || (new_group != null && !l.gowner.equals(new_group))) {
			l.owner = new_owner;
			if (new_group != null)
				l.gowner = new_group;
			if (!l.update(true, false, null)) {
				lfns_error.add(l);
				return false;
			}
		}

		lfns_ok.add(l);

		return true;
	}

}
