/**
 * 
 */
package alien.io.protocols;

/**
 * @author costing
 * @since Dec 8, 2010
 */
public final class Factory {

	/**
	 * Normal (xrdcp) transfers
	 */
	public static final Xrootd xrootd = new Xrootd();

	/**
	 * Normal (xrdcp) transfers for LFN_CSDs
	 */
	public static final XrootdCsd xrootdcsd = new XrootdCsd();

	/**
	 * Third-party xrootd transfers
	 */
	public static final Xrd3cp xrd3cp = new Xrd3cp();

	/**
	 * Third-party xrootd transfers with the new protocol available in Xrootd 4+ client and server
	 */
	public static final Xrd3cp4 xrd3cp4 = new Xrd3cp4();

	/**
	 * Thrird-party xrootd transfers through a set of well-connected gateway servers at CERN (so not real 3rd party but still preferable to transferring through the agents)
	 */
	public static final Xrd3cpGW xrd3cpGW = new Xrd3cpGW();

	/**
	 * HTTP protocol
	 */
	public static final Http http = new Http();

	/**
	 * Torrent protocol
	 */
	public static final Torrent torrent = new Torrent();

	/**
	 * local cp protocol
	 */
	public static final CpForTest cp = new CpForTest();
}
