package alien.catalogue;

import java.io.File;
import java.io.FileReader;
import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.logging.Logger;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import alien.config.ConfigUtils;
import alien.monitoring.Monitor;
import alien.monitoring.MonitorFactory;

/**
 * LFN implementation for FS-based JSON files catalogue
 */
public class LFN_JSON implements Comparable<LFN_JSON>, CatalogEntity {

	/**
	 *
	 */
	private static final long serialVersionUID = 9158990164379160998L;

	/**
	 * Logger
	 */
	static transient final Logger logger = ConfigUtils.getLogger(LFN_JSON.class.getCanonicalName());

	/**
	 * Monitoring component
	 */
	static transient final Monitor monitor = MonitorFactory.getMonitor(LFN_JSON.class.getCanonicalName());

	/**
	 * Owner
	 */
	public String owner;

	/**
	 * Last change timestamp
	 */
	public Date ctime;

	/**
	 * Size, in bytes
	 */
	public long size;

	/**
	 * Group
	 */
	public String gowner;

	/**
	 * File type
	 */
	public char type;

	/**
	 * Access rights
	 */
	public String perm;

	/**
	 * The unique identifier
	 */
	public UUID guid;

	/**
	 * MD5 checksum
	 */
	public String md5;

	/**
	 * Whether or not this entry really exists in the catalogue
	 */
	public boolean exists = true;

	/**
	 * Parent directory
	 */
	public LFN_JSON parentDir = null;

	/**
	 * Canonical path
	 */
	private String canonicalName;

	/**
	 * short name
	 */
	private String lfn;

	/**
	 * Job ID that produced this file
	 *
	 * @since AliEn 2.19
	 */
	public int jobid;

	/**
	 * physical locations
	 */
	private Set<String> collectionMembers = null;

	/**
	 * physical locations
	 */
	private Set<PFN_JSON> pfnCache = null;

	/**
	 * lfns in the archive, if type is 'a'
	 */
	private Set<ZIPM> zipMembers = null;

	/**
	 * Auxiliary class to store zip members (partial lfns) we could also create LFNs, but we don't need for now
	 */
	public static class ZIPM implements Serializable {
		/**
		 * 
		 */
		private static final long serialVersionUID = -4603310052771826537L;

		/**
		 * LFN name
		 */
		final String lfnName;

		/**
		 * File size, in bytes
		 */
		final long fileSize;

		/**
		 * MD5 checksum of the contents
		 */
		final String md5Sum;

		/**
		 * @param l
		 * @param si
		 * @param md5sum
		 */
		public ZIPM(final String l, final long si, final String md5sum) {
			lfnName = l;
			fileSize = si;
			md5Sum = md5sum;
		}

		@Override
		public String toString() {
			return "[LFN: " + lfnName + " - Size: " + fileSize + " - md5: " + md5Sum + "]";
		}
	}

	/**
	 * Auxiliary class to store pfns we don't need to load senumbers, guids, caches...
	 */
	public static class PFN_JSON implements Serializable {
		/**
		 * 
		 */
		private static final long serialVersionUID = 4199659163458119586L;

		/**
		 * PFN URL
		 */
		String pfn;

		/**
		 * SE name
		 */
		String seName;

		/**
		 * @param pfnstring
		 * @param senamestring
		 */
		public PFN_JSON(final String pfnstring, final String senamestring) {
			pfn = pfnstring;
			seName = senamestring;
		}

		@Override
		public String toString() {
			return "[PFN: " + pfn + " - SE: " + seName + "]";
		}
	}

	/**
	 * @param canonicalLFN
	 */
	public LFN_JSON(final String canonicalLFN) {
		this.canonicalName = canonicalLFN;

		if (canonicalLFN == null || canonicalLFN.length() == 0) {
			exists = false;
			return;
		}

		if (canonicalLFN.endsWith("/"))
			// Is a directory
			this.type = 'd';
		else {
			lfn = canonicalLFN.substring(canonicalLFN.lastIndexOf("/") + 1, canonicalLFN.length());

			try (FileReader lfnfile = new FileReader(canonicalLFN)) {
				final JSONParser parser = new JSONParser();

				final Object obj = parser.parse(lfnfile);

				final JSONObject jsonObject = (JSONObject) obj;

				this.type = 'f';
				this.guid = UUID.fromString((String) jsonObject.get("guid"));

				final String date = (String) jsonObject.get("ctime"); // 2013-07-26 10:30:02
				final SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
				this.ctime = formatter.parse(date);

				this.md5 = (String) jsonObject.get("md5");
				this.perm = (String) jsonObject.get("perm");
				this.jobid = Integer.parseInt(((String) jsonObject.get("jobid")));
				this.size = Long.parseLong(((String) jsonObject.get("size")));
				this.owner = (String) jsonObject.get("owner");
				this.gowner = (String) jsonObject.get("gowner");

				// arrays: zip_members, lfns, pfns
				final JSONArray pfns = (JSONArray) jsonObject.get("pfns");
				if (pfns != null && pfns.size() > 0) {
					pfnCache = new LinkedHashSet<>();

					for (int i = 0; i < pfns.size(); i++) {
						final JSONObject pfno = (JSONObject) pfns.get(i);

						final String pfnstring = (String) pfno.get("pfn");
						final String se = (String) pfno.get("se");

						pfnCache.add(new PFN_JSON(pfnstring, se));
					}
				}

				final JSONArray mem = (JSONArray) jsonObject.get("zip_members");
				if (mem != null && mem.size() > 0) {
					this.type = 'a';
					zipMembers = new LinkedHashSet<>();
					for (int i = 0; i < mem.size(); i++) {
						final JSONObject zipmem = (JSONObject) mem.get(i);

						final String lfnName = (String) zipmem.get("lfn");
						final String md5Sum = (String) zipmem.get("md5");
						final long fileSize = Long.parseLong((String) zipmem.get("size"));

						if (this.lfn.equals(lfnName)) {
							this.type = 'm';
							this.size = fileSize;
							this.md5 = md5Sum;
						}

						zipMembers.add(new ZIPM(lfnName, fileSize, md5Sum));
					}
				}

				final JSONArray colmembers = (JSONArray) jsonObject.get("lfns");
				if (colmembers != null && colmembers.size() > 0) {
					this.type = 'c';
					collectionMembers = new LinkedHashSet<>();
					for (int i = 0; i < colmembers.size(); i++)
						collectionMembers.add((String) colmembers.get(i));
				}

			}
			catch (final Exception e) {
				e.printStackTrace();
			}
		}
	}

	@Override
	public String toString() {
		String str = "LFN: " + canonicalName + "\n - Type: " + type;

		if (type != 'd') {
			str += "\n - Size: " + size + "\n - GUID: " + guid + "\n - md5: " + md5 + "\n - Perm: " + perm + "\n - Lfn: " + lfn + "\n - Owner: " + owner + "\n - Gowner: " + gowner + "\n - JobId: "
					+ jobid;
			if (collectionMembers != null)
				str += "\n - collectionMembers: " + collectionMembers.toString();
			if (pfnCache != null)
				str += "\n - pfns: " + pfnCache.toString();
			if (zipMembers != null)
				str += "\n - zipMembers: " + zipMembers.toString();
		}

		return str;
	}

	@Override
	public String getOwner() {
		return owner;
	}

	@Override
	public String getGroup() {
		return gowner;
	}

	@Override
	public String getPermissions() {
		return perm != null ? perm : "755";
	}

	@Override
	public String getName() {
		return lfn;
	}

	/**
	 * Get the canonical name (full path and name)
	 *
	 * @return canonical name
	 */
	public String getCanonicalName() {
		return canonicalName;
	}

	@Override
	public char getType() {
		return type;
	}

	@Override
	public long getSize() {
		return size;
	}

	@Override
	public String getMD5() {
		return md5;
	}

	@Override
	public int compareTo(final LFN_JSON o) {
		if (this == o)
			return 0;

		return canonicalName.compareTo(o.canonicalName);
	}

	@Override
	public boolean equals(final Object obj) {
		if (obj == null || !(obj instanceof LFN_JSON))
			return false;

		if (this == obj)
			return true;

		final LFN_JSON other = (LFN_JSON) obj;

		return compareTo(other) == 0;
	}

	/**
	 * @return parent directory
	 */
	public LFN_JSON getParentDir() {
		if (parentDir != null)
			return parentDir;

		if (canonicalName.length() > 1) {
			int idx = canonicalName.lastIndexOf('/');

			if (idx == canonicalName.length() - 1)
				idx = canonicalName.lastIndexOf('/', idx - 1);

			if (idx >= 0)
				parentDir = new LFN_JSON(canonicalName.substring(0, idx + 1));
		}

		return parentDir;
	}

	@Override
	public int hashCode() {
		return Integer.parseInt(perm) * 13 + canonicalName.hashCode() * 17;
	}

	/**
	 * is this LFN a directory
	 *
	 * @return <code>true</code> if this LFN is a directory
	 */
	public boolean isDirectory() {
		return (type == 'd');
	}

	/**
	 * @return <code>true</code> if this LFN points to a file
	 */
	public boolean isFile() {
		return (type == 'f' || type == '-');
	}

	/**
	 * @return <code>true</code> if this is a native collection
	 */
	public boolean isCollection() {
		return type == 'c';
	}

	/**
	 * @return <code>true</code> if this is a a member of an archive
	 */
	public boolean isMemberOfArchive() {
		return type == 'm';
	}

	/**
	 * @return <code>true</code> if this is an archive
	 */
	public boolean isArchive() {
		return type == 'a';
	}

	/**
	 * @return the list of entries in this folder
	 */
	public List<LFN_JSON> list() {
		final List<LFN_JSON> ret = new ArrayList<>();

		if (type != 'd')
			ret.add(this);
		else {
			// Do list with java
			final File folder = new File(canonicalName);

			final File[] listing = folder.listFiles();

			if (listing != null) {
				for (final File fileEntry : listing) {
					String lfnjson = canonicalName + fileEntry.getName();
					if (fileEntry.isDirectory())
						lfnjson += "/";

					ret.add(new LFN_JSON(lfnjson));
				}
			}

			// String ls = Utils.getOutput("ls -ldp "+canonicalName+"*"); // TODO: de-hardcode the location
			// ArrayList<String> parts = new ArrayList<String>(Arrays.asList( ls.split("\n") ));
			// for (String value : parts){
			// String[] ls_parts = value.split("\\s+");
			// String lfn = ls_parts[ls_parts.length-1];
			// if(!lfn.contains("/")) // links
			// lfn = canonicalName+lfn;
			//
			// System.out.println("Creating: "+lfn); //TODELETE
			// ret.add( new LFN_JSON(lfn) );
			// }

		}

		return ret;
	}

	/**
	 * @return the set of files in this collection, or <code>null</code> if this is not a collection
	 */
	public Set<String> listCollection() {
		if (!isCollection() || !exists)
			return null;

		return collectionMembers;
	}

	/**
	 * @return physical locations of the file
	 */
	public Set<PFN_JSON> whereis() {
		if (!exists || guid == null)
			return null;

		return pfnCache;
	}

}
