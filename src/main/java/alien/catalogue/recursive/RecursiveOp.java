package alien.catalogue.recursive;

import java.util.Set;
import java.util.TreeSet;

import alien.catalogue.LFN_CSD;
import alien.user.AliEnPrincipal;

/**
 * @author mmmartin
 *
 */
public abstract class RecursiveOp {
	/**
	 * Entries that were ok
	 */
	final Set<LFN_CSD> lfns_ok = new TreeSet<>();
	/**
	 * Entries that the operation could not be applied on
	 */
	final Set<LFN_CSD> lfns_error = new TreeSet<>();
	private boolean recurse_infinitely = false;
	private boolean onlyAppend = false;
	/**
	 * Identity to set
	 */
	AliEnPrincipal user = null;
	/**
	 * Target
	 */
	LFN_CSD lfnc_target = null;
	/**
	 * Parent
	 */
	LFN_CSD lfnc_target_parent = null;
	/**
	 * Change to this owner
	 */
	String new_owner = null;
	/**
	 * And this group
	 */
	String new_group = null;

	/**
	 * @param lfnc
	 * @return true if no problem and recursion can continue
	 */
	public abstract boolean callback(LFN_CSD lfnc);

	/**
	 * @return recurse_infinitely
	 */
	public boolean getRecurseInfinitely() {
		return recurse_infinitely;
	}

	/**
	 * @param ri
	 */
	public void setRecurseInfinitely(final boolean ri) {
		this.recurse_infinitely = ri;
	}

	/**
	 * @return onlyAppend
	 */
	public boolean getOnlyAppend() {
		return onlyAppend;
	}

	/**
	 * @param oa
	 */
	public void setOnlyAppend(final boolean oa) {
		this.onlyAppend = oa;
	}

	/**
	 * @return user
	 */
	public AliEnPrincipal getuser() {
		return user;
	}

	/**
	 * @param us
	 */
	public void setUser(final AliEnPrincipal us) {
		this.user = us;
	}

	/**
	 * @return lfn_target
	 */
	public LFN_CSD getLfnTarget() {
		return lfnc_target;
	}

	/**
	 * @param lfnct
	 */
	public void setLfnTarget(final LFN_CSD lfnct) {
		this.lfnc_target = lfnct;
	}

	/**
	 * @return user
	 */
	public LFN_CSD getLfnTargetParent() {
		return lfnc_target_parent;
	}

	/**
	 * @param lfnctp
	 */
	public void setLfnTargetParent(final LFN_CSD lfnctp) {
		this.lfnc_target_parent = lfnctp;
	}

	/**
	 * @return lfns_ok
	 */
	public Set<LFN_CSD> getLfnsOk() {
		return lfns_ok;
	}

	/**
	 * @return lfns_error
	 */
	public Set<LFN_CSD> getLfnsError() {
		return lfns_error;
	}

	/**
	 * @return new_owner
	 */
	public String getNewOwner() {
		return new_owner;
	}

	/**
	 * @param no
	 */
	public void setNewOwner(final String no) {
		new_owner = no;
	}

	/**
	 * @return new_group
	 */
	public String getNewGroup() {
		return new_group;
	}

	/**
	 * @param ng
	 */
	public void setNewGroup(final String ng) {
		new_group = ng;
	}

}
