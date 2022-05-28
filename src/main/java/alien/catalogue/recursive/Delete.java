package alien.catalogue.recursive;

import alien.catalogue.LFN_CSD;
import alien.user.AuthorizationChecker;

/**
 * @author mmmartin
 *
 */
public class Delete extends RecursiveOp {

	@Override
	public boolean callback(LFN_CSD l) {
		// check permissions to rm
		if (!AuthorizationChecker.canWrite(l, user)) {
			lfns_error.add(l);
			return false;
		}
		// rm and add to the final collection of lfns
		if (!l.delete(true, true, true)) {
			lfns_error.add(l);
			return false;
		}

		lfns_ok.add(l);

		return true;
	}

}
