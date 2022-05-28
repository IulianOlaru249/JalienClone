package alien.api;

/**
 * @author costing
 *
 */
public interface Cacheable {

	/**
	 * @return unique key
	 */
	public String getKey();

	/**
	 * @return lifetime, in milliseconds
	 */
	public long getTimeout();

}
