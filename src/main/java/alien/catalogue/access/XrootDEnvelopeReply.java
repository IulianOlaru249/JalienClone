package alien.catalogue.access;

import java.io.Serializable;
import java.util.StringTokenizer;
import java.util.UUID;

import alien.catalogue.GUID;
import alien.catalogue.GUIDUtils;
import alien.catalogue.PFN;
import alien.se.SE;
import alien.se.SEUtils;

/**
 * @author ron
 * 
 */
public class XrootDEnvelopeReply implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 213489317746462701L;

	/**
	 * pfn of the file on the SE (proto:://hostdns:port//storagepath)
	 */
	public final PFN pfn;

	/**
	 * Signed envelope
	 */
	protected String signedEnvelope;

	/**
	 * Create a signed only envelope in order to verify it
	 * 
	 * @param envelope
	 */
	public XrootDEnvelopeReply(String envelope) {

		StringTokenizer st = new StringTokenizer(envelope, "\\&");
		String spfn = "";
		String guid = "";
		String se = "";
		long size = 0;
		String md5 = "";

		while (st.hasMoreTokens()) {
			String tok = st.nextToken();

			int idx = tok.indexOf('=');

			if (idx >= 0) {
				String key = tok.substring(0, idx);
				String value = tok.substring(idx + 1);

				if ("path".equals(key))
					spfn = value;
				else if ("size".equals(key))
					size = Long.parseLong(value);
				else if ("md5".equals(key))
					md5 = value;
				else if ("se".equals(key))
					se = value;
			}
		}

		if ("alice::cern::testse".equals(se.toLowerCase()))
			se = "alice::cern::setest";

		final SE rSE = SEUtils.getSE(se);

		System.out.println("pfn: " + spfn + " guid: " + guid + " size: " + size
				+ " md5: " + md5 + " se: " + se);

		final GUID g = GUIDUtils.getGUID(
				UUID.fromString(spfn.substring(spfn.lastIndexOf('/') + 1)), true);
		g.md5 = md5;
		g.size = size;

		if (rSE != null && rSE.seioDaemons != null && rSE.seioDaemons.length() > 0)
			spfn = rSE.seioDaemons + "/" + spfn;

		System.out.println("pfn: " + spfn + " guid: " + guid + " size: " + size + " md5: " + md5);

		System.out.println(" se: " + (rSE != null ? rSE.seName : "null"));

		this.pfn = new PFN(spfn, g, SEUtils.getSE(se));

		signedEnvelope = envelope;
	}

}
