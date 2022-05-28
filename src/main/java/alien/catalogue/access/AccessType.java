package alien.catalogue.access;

/**
 * @author costing
 * 
 */
public enum AccessType {

	/**
	 * Read
	 */
	READ("read"),
	/**
	 * Write
	 */
	WRITE("write"),
	/**
	 * Delete
	 */
	DELETE("delete"),
	/**
	 * NULL
	 */
	NULL("null");

	private final String shortDescription;

	AccessType(final String shortDescription) {
		this.shortDescription = shortDescription;
	}

	@Override
	public String toString() {
		return shortDescription;
	}

}
