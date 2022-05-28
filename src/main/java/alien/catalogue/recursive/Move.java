package alien.catalogue.recursive;

import alien.catalogue.LFN_CSD;
import alien.user.AuthorizationChecker;

/**
 * @author mmmartin
 *
 */
public class Move extends RecursiveOp {

	@Override
	public boolean callback(LFN_CSD l) {
		// check permissions to mv
		if (!AuthorizationChecker.canWrite(l, user)) {
			lfns_error.add(l);
			return false;
		}
		// mv and add to the final collection of lfns
		if (LFN_CSD.mv(l, lfnc_target, lfnc_target_parent) == null) {
			lfns_error.add(l);
			return false;
		}

		lfns_ok.add(l);

		return true;
	}

}
