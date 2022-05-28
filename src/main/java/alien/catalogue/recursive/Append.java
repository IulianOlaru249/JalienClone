package alien.catalogue.recursive;

import alien.catalogue.LFN_CSD;

/**
 * @author mmmartin
 *
 */
public class Append extends RecursiveOp {

	@Override
	public boolean callback(LFN_CSD lfnc) {
		lfns_ok.add(lfnc);
		return true;
	}
}

// In AliEn the sorting was repeatable and this flag made sense
// choose to use sorted/unsorted type according to flag (-s)
// if ((flags & LFNCSDUtils.FIND_NO_SORT) != 0)
// ret = new LinkedHashSet<>();
// else
// ret = new TreeSet<>();
