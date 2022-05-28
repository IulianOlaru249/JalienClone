package alien.catalogue.access;

import java.io.Serializable;

/**
 * Generic envelope
 * 
 * @author costing
 */
public class AccessTicket implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = -6200985047035616911L;

	/**
	 * Access type
	 */
	public final AccessType type;

	/**
	 * XrootD envelope
	 */
	public final XrootDEnvelope envelope;

	/**
	 * @param type
	 * @param envelope
	 */
	public AccessTicket(final AccessType type, final XrootDEnvelope envelope) {
		this.type = type;
		this.envelope = envelope;
	}

	/**
	 * @return access type, see the constants of this class
	 */
	public AccessType getAccessType() {
		return type;
	}
}
