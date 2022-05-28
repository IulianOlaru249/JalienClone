package alien.test.cassandra;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

import alien.catalogue.CatalogEntity;
import alien.catalogue.LFN;
import alien.config.ConfigUtils;
import alien.monitoring.Monitor;
import alien.monitoring.MonitorFactory;

/**
 * LFN implementation for Cassandra
 */
public class LFN_CSD_OLD implements Comparable<LFN_CSD_OLD>, CatalogEntity {
	/**
	 *
	 */
	private static final long serialVersionUID = 9158990164379160910L;

	/**
	 * Logger
	 */
	static transient final Logger logger = ConfigUtils.getLogger(LFN_CSD_OLD.class.getCanonicalName());

	/**
	 * Monitoring component
	 */
	static transient final Monitor monitor = MonitorFactory.getMonitor(LFN_CSD_OLD.class.getCanonicalName());

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
	 * MD5 checksum
	 */
	public String checksum;

	/**
	 * Whether or not this entry really exists in the catalogue
	 */
	public boolean exists = true;

	/**
	 * Canonical path
	 */
	public String canonicalName;

	/**
	 * Parent name
	 */
	public String path;

	/**
	 * Child name
	 */
	public String child;

	/**
	 * guid
	 */
	public UUID guid;

	/**
	 * Job ID that produced this file
	 *
	 * @since AliEn 2.19
	 */
	public Long jobid;

	/**
	 * physical/logical locations
	 */
	public HashMap<Integer, String> pfns = null;

	/**
	 * physical locations
	 */
	public HashMap<String, String> metadata = null;

	/**
	 * @param l
	 */
	public LFN_CSD_OLD(LFN l) {
		canonicalName = l.getCanonicalName();

		int remove = 0;
		int idx = canonicalName.lastIndexOf('/');
		if (idx == canonicalName.length() - 1) {
			idx = canonicalName.lastIndexOf('/', idx - 1);
			remove = 1;
		}
		path = canonicalName.substring(0, idx + 1); // parent dir without trailing slash
		child = canonicalName.substring(idx + 1, canonicalName.length() - remove); // last part of path without trailing slash

		size = l.getSize();
		jobid = Long.valueOf(l.jobid);
		checksum = l.getMD5();
		type = l.getType();
		perm = l.getPermissions();
		ctime = l.ctime;
		owner = l.getOwner();
		gowner = l.getGroup();
		guid = l.guid;
		metadata = new HashMap<>();
	}

	/**
	 * @param lfn
	 * @param getFromDB
	 */
	public LFN_CSD_OLD(String lfn, boolean getFromDB) {
		canonicalName = lfn;

		int remove = 0;
		int idx = canonicalName.lastIndexOf('/');
		if (idx == canonicalName.length() - 1) {
			idx = canonicalName.lastIndexOf('/', idx - 1);
			remove = 1;
		}

		path = canonicalName.substring(0, idx + 1); // parent dir without trailing slash
		child = canonicalName.substring(idx + 1, canonicalName.length() - remove); // last part of path without trailing slash

		if (getFromDB) {
			try {
				@SuppressWarnings("resource")
				final Session session = DBCassandra.getInstance();
				if (session == null)
					return;

				PreparedStatement statement = session.prepare("select * from catalogue.lfns where path = ? and child = ?");
				BoundStatement boundStatement = new BoundStatement(statement);
				boundStatement.bind(this.path, this.child);

				boundStatement.setConsistencyLevel(ConsistencyLevel.QUORUM);

				ResultSet results = session.execute(boundStatement);
				init(results.one());
			}
			catch (Exception e) {
				System.err.println("Exception trying to create LFN_CSD: " + e);
				return;
			}
		}
	}

	/**
	 * @param row
	 */
	public LFN_CSD_OLD(Row row) {
		init(row);
	}

	private void init(Row row) {
		if (row == null || row.isNull("path")) {
			logger.log(Level.SEVERE, "Row null creating LFN_CSD ");
			exists = false;
			return;
		}

		try {
			pfns = (HashMap<Integer, String>) row.getMap("pfns", Integer.class, String.class);
			metadata = (HashMap<String, String>) row.getMap("metadata", String.class, String.class);
			path = row.getString("path");
			child = row.getString("child");
			type = row.getString("type").charAt(0);
			checksum = row.getString("checksum");
			perm = row.getString("perm");
			jobid = Long.valueOf(row.getLong("jobid"));
			size = row.getLong("size");
			ctime = row.getTimestamp("ctime");
			owner = row.getString("owner");
			gowner = row.getString("gowner");
			guid = row.getUUID("guid");
			canonicalName = path + child;
			if (type == 'd')
				canonicalName += "/";
		}
		catch (Exception e) {
			logger.log(Level.SEVERE, "Can't create LFN_CSD from row: " + e);
		}
	}

	@Override
	public String toString() {
		String str = "LFN (" + canonicalName + "): " + path + " " + child + "\n - Type: " + type;

		// if (type != 'd') {
		str += "\n - Size: " + size + "\n - Checksum: " + checksum + "\n - Perm: " + perm + "\n - Owner: " + owner + "\n - Gowner: " + gowner + "\n - JobId: " + jobid + "\n - Guid: " + guid
				+ "\n - Ctime: " + ctime;
		if (pfns != null)
			str += "\n - pfns: " + pfns.toString();
		if (metadata != null)
			str += "\n - metadata: " + metadata.toString();
		// }

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
		return canonicalName;
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
		return checksum;
	}

	@Override
	public int compareTo(final LFN_CSD_OLD o) {
		if (this == o)
			return 0;

		return canonicalName.compareTo(o.canonicalName);
	}

	@Override
	public boolean equals(final Object obj) {
		if (obj == null || !(obj instanceof LFN_CSD_OLD))
			return false;

		if (this == obj)
			return true;

		final LFN_CSD_OLD other = (LFN_CSD_OLD) obj;

		return compareTo(other) == 0;
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

	public List<LFN_CSD_OLD> list() {
		return list(null, null);
	}

	/**
	 * @param table
	 * @param level
	 * @return list of lfns
	 */
	public List<LFN_CSD_OLD> list(String table, ConsistencyLevel level) {
		if (!exists)
			return null;

		if (monitor != null)
			monitor.incrementCounter("LFN_CSD_list");

		final List<LFN_CSD_OLD> ret = new ArrayList<>();
		if (type != 'd') {
			ret.add(this);
			return ret;
		}

		String t = "catalogue.lfns";
		if (table != null) {
			t = table;
		}

		ConsistencyLevel cl = ConsistencyLevel.QUORUM;
		if (level != null)
			cl = level;

		try {
			@SuppressWarnings("resource")
			final Session session = DBCassandra.getInstance();
			if (session == null)
				return null;

			PreparedStatement statement = session.prepare("select * from " + t + " where path = ?");
			BoundStatement boundStatement = new BoundStatement(statement);
			boundStatement.bind(this.canonicalName);

			boundStatement.setConsistencyLevel(cl);

			ResultSet results = session.execute(boundStatement);
			for (Row row : results) {
				ret.add(new LFN_CSD_OLD(row));
			}
		}
		catch (Exception e) {
			System.err.println("Exception trying to whereis: " + e);
			return null;
		}

		return ret;
	}

	// /**
	// * @return find matching lfns by name and metadata in the hierarchy
	// */
	//
	// public static List<LFN_CSD_OLD> find(String base, String pattern, String parameters, String metadata) {
	// return find(base, pattern, parameters, metadata, null, null);
	// }
	//
	// public static List<LFN_CSD_OLD> find(String base, String pattern, String parameters, String metadata, String table, ConsistencyLevel level) {
	// if (monitor != null)
	// monitor.incrementCounter("LFN_CSD_find");
	//
	// final List<LFN_CSD_OLD> ret = new ArrayList<>();
	//
	// LFN_CSD_OLD baselfn = new LFN_CSD_OLD(base, true);
	// if (!baselfn.exists || baselfn.type != 'd')
	// return null;
	//
	// pattern = Format.replace(pattern, "*", ".*");
	// Pattern p = Pattern.compile(pattern);
	//
	// List<LFN_CSD_OLD> ls = baselfn.list();
	// List<LFN_CSD_OLD> new_entries = new ArrayList<>();
	//
	// boolean end_reached = ls.size() <= 0;
	// while (!end_reached) {
	// for (LFN_CSD_OLD l : ls) {
	//
	// if (l.type == 'd') {
	// new_entries.add(l);
	// continue;
	// }
	//
	// Matcher m = p.matcher(l.child);
	// if (m.find())
	// ret.add(l);
	// }
	// end_reached = new_entries.size() <= 0 || ret.size() >= 50;
	// ls.clear();
	// ls.addAll(new_entries);
	// new_entries.clear();
	// }
	//
	// return ret;
	// }

	/**
	 * @return physical locations of the file
	 */
	public HashMap<Integer, String> whereis() {
		return whereis(null, null);
	}

	/**
	 * @param table
	 * @param level
	 * @return physical locations of the file
	 */
	public HashMap<Integer, String> whereis(String table, ConsistencyLevel level) {
		if (!exists)
			return null;

		String t = "catalogue.lfns";
		if (table != null) {
			t = table;
		}

		ConsistencyLevel cl = ConsistencyLevel.QUORUM;
		if (level != null)
			cl = level;

		try {
			@SuppressWarnings("resource")
			final Session session = DBCassandra.getInstance();
			if (session == null)
				return null;

			PreparedStatement statement = session.prepare("select pfns from " + t + " where path = ? and child = ?");
			BoundStatement boundStatement = new BoundStatement(statement);
			boundStatement.bind(this.path, this.child);

			boundStatement.setConsistencyLevel(cl);

			ResultSet results = session.execute(boundStatement);
			for (Row row : results) {
				pfns = (HashMap<Integer, String>) row.getMap("pfns", Integer.class, String.class);
			}
		}
		catch (Exception e) {
			System.err.println("Exception trying to whereis: " + e);
			return null;
		}

		return pfns;
	}

	/**
	 * @return insertion result
	 */
	public boolean insert() {
		return this.insert(null, null, null);
	}

	/**
	 * @param table_lfns
	 * @param table_se_lookup
	 * @param level
	 * @return insertion result
	 */
	public boolean insert(String table_lfns, String table_se_lookup, ConsistencyLevel level) {
		// lfn | ctime | dir | gowner | jobid | link | md5 | owner | perm | pfns
		// | size | type
		String t = "catalogue.lfns";
		if (table_lfns != null)
			t = table_lfns;

		String ts = "catalogue.se_lookups";
		if (table_se_lookup != null)
			ts = table_se_lookup;

		ConsistencyLevel cl = ConsistencyLevel.QUORUM;
		if (level != null) {
			cl = level;
		}

		try {
			@SuppressWarnings("resource")
			final Session session = DBCassandra.getInstance();
			if (session == null)
				return false;

			PreparedStatement statement;
			BoundStatement boundStatement;

			if (type == 'a' || type == 'f') {
				statement = session
						.prepare("INSERT INTO " + t + " (path, child, ctime, gowner, jobid, checksum, owner, perm, pfns, size, type, metadata, guid)" + " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)");
				boundStatement = new BoundStatement(statement);
				boundStatement.bind(path, child, ctime, gowner, jobid, checksum, owner, perm, pfns, Long.valueOf(size), String.valueOf(type), metadata, guid);
			}
			else if (type == 'm' || type == 'l') {
				statement = session.prepare("INSERT INTO " + t + " (path, child, ctime, gowner, jobid, checksum, owner, perm, pfns, size, type, metadata)" + " VALUES (?,?,?,?,?,?,?,?,?,?,?,?)");
				boundStatement = new BoundStatement(statement);
				boundStatement.bind(path, child, ctime, gowner, jobid, checksum, owner, perm, pfns, Long.valueOf(size), String.valueOf(type), metadata);
			}
			else { // 'd'
				statement = session.prepare("INSERT INTO " + t + " (path, child, ctime, gowner, jobid, checksum, owner, perm, size, type)" + " VALUES (?,?,?,?,?,?,?,?,?,?)");
				boundStatement = new BoundStatement(statement);
				boundStatement.bind(path, child, ctime, gowner, jobid, checksum, owner, perm, Long.valueOf(size), String.valueOf(type));
			}

			boundStatement.setConsistencyLevel(cl);
			session.execute(boundStatement);

			// Insert files and archives into se_lookups
			if (type == 'a' || type == 'f') {
				Set<Integer> seNumbers = pfns.keySet();
				for (Integer seNumber : seNumbers) {
					statement = session.prepare("INSERT INTO " + ts + " (seNumber, guid, lfn, size, owner)" + " VALUES (?,?,?,?,?)");
					boundStatement = new BoundStatement(statement);
					boundStatement.bind(seNumber, guid, path + child, Long.valueOf(size), owner);
					boundStatement.setConsistencyLevel(cl);
					session.execute(boundStatement);
				}
			}
		}
		catch (Exception e) {
			System.err.println("Exception trying to insert: " + e);
			return false;
		}

		return true;
	}

	/**
	 * @param folder
	 * @param table
	 * @param level
	 * @return create a directory hierarchy
	 */
	public static boolean createDirectory(String folder, String table, ConsistencyLevel level) {
		// We want to create the whole hierarchy upstream
		// check if is already there

		if (folder.length() <= 1 || existsLfn(folder, table, level))
			return true;

		// get parent and create it if doesn't exist
		int remove = 0;
		int idx = folder.lastIndexOf('/');
		if (idx == folder.length() - 1) {
			idx = folder.lastIndexOf('/', idx - 1);
			remove = 1;
		}
		String path = folder.substring(0, idx); // parent dir without trailing slash
		String child = folder.substring(idx + 1, folder.length() - remove); // last part of path without trailing slash

		createDirectory(path, table, level);

		LFN_CSD_OLD newdir = new LFN_CSD_OLD(path + child, false); // (path, child, ctime, gowner, jobid, link, md5, owner, perm, size, type)
		newdir.type = 'd';
		newdir.path = path + "/";
		newdir.child = child;
		newdir.ctime = new Date();
		newdir.gowner = "aliprod";
		newdir.owner = "aliprod";
		newdir.jobid = Long.valueOf(0l);
		newdir.checksum = "";
		newdir.perm = "755";
		newdir.size = 0;

		return newdir.insert(table, null, level);
	}

	@SuppressWarnings("resource")
	private static boolean existsLfn(String lfn, String table, ConsistencyLevel level) {
		String t = "catalogue.lfns";
		if (table != null) {
			t = table;
		}

		ConsistencyLevel cl = ConsistencyLevel.QUORUM;
		if (level != null)
			cl = level;

		try {

			final Session session = DBCassandra.getInstance();
			if (session == null)
				return false;

			PreparedStatement statement = session.prepare("select path from " + t + " where path = ?");
			BoundStatement boundStatement = new BoundStatement(statement);
			boundStatement.bind(lfn);

			boundStatement.setConsistencyLevel(cl);

			ResultSet results = session.execute(boundStatement);
			return results.getAvailableWithoutFetching() > 0;
		}
		catch (Exception e) {
			System.err.println("Exception trying to check existsLfn: " + e);
			return false;
		}
	}

}
