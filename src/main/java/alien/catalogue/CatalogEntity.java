/**
 * 
 */
package alien.catalogue;

import java.io.Serializable;

/**
 * @author costing
 * @since Nov 11, 2010
 */
public interface CatalogEntity extends Serializable {

	/**
	 * @return owner of the entity
	 */
	public String getOwner();

	/**
	 * @return group owner of the entity
	 */
	public String getGroup();

	/**
	 * @return permissions, in Unix style (such as 755)
	 */
	public String getPermissions();

	/**
	 * @return name of the entity
	 */
	public String getName();

	/**
	 * @return entry type (f,c,d,l)
	 */
	public char getType();

	/**
	 * @return entity size in bytes
	 */
	public long getSize();

	/**
	 * @return the MD5 checksum of the content of this entity
	 */
	public String getMD5();
}
