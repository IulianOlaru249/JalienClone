/**
 *
 */
package alien.io.protocols;

import java.io.IOException;

import alien.catalogue.PFN;

/**
 * 3rd party Xrootd transfers using the default client in Xrootd 4+
 *
 * @author costing
 * @since Jun 16 2015
 */
public class Xrd3cp4 extends Xrootd {

	/**
	 *
	 */
	private static final long serialVersionUID = 9084272684664087714L;

	/**
	 * package protected
	 */
	Xrd3cp4() {
		// package protected
	}

	@Override
	public String transfer(final PFN source, final PFN target) throws IOException {
		return transferv4(source, target, TPC_ONLY);
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "xrd3cp4";
	}

	@Override
	int getPreference() {
		return 2;
	}

	@Override
	public boolean isSupported() {
		return xrootdNewerThan4;
	}

	@Override
	public byte protocolID() {
		return 5;
	}
}
