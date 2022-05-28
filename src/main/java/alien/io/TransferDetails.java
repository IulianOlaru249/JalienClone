package alien.io;

import java.io.Serializable;
import java.util.Date;

import lazyj.DBFunctions;
import lia.util.StringFactory;

/**
 * Wrapper around one row in TRANSFERS_DIRECT
 * 
 * @author costing
 */
public class TransferDetails implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -6060306605081290001L;

	/**
	 * transfer queue id
	 */
	public final int transferId;

	/**
	 * priority
	 */
	public final int priority;

	/**
	 * change time
	 */
	public final Date ctime;

	/**
	 * status
	 */
	public final String status;

	/**
	 * jdl
	 */
	public final String jdl;

	/**
	 * LFN
	 */
	public final String lfn;

	/**
	 * file size
	 */
	public final long size;

	/**
	 * error code
	 */
	public final int error;

	/**
	 * started timestamp
	 */
	public final long started;

	/**
	 * sent timestamp
	 */
	public final long sent;

	/**
	 * finished timestamp
	 */
	public final long finished;

	/**
	 * received timestamp
	 */
	public final long received;

	/**
	 * expires
	 */
	public final int expires;

	/**
	 * transfer group
	 */
	public final int transferGroup;

	/**
	 * transfer options
	 */
	public final String options;

	/**
	 * target SE
	 */
	public final String destination;

	/**
	 * user who has submitted it
	 */
	public final String user;

	/**
	 * attempts
	 */
	public final int attempts;

	/**
	 * protocols
	 */
	public final String protocols;

	/**
	 * type
	 */
	public final String type;

	/**
	 * agent id
	 */
	public final int agentid;

	/**
	 * failure reason
	 */
	public final String reason;

	/**
	 * pfn
	 */
	public final String pfn;

	/**
	 * protocol ID
	 */
	public final String protocolid;

	/**
	 * FTD instance
	 */
	public final String ftd;

	/**
	 * ?!
	 */
	public final int persevere;

	/**
	 * retry time (?!)
	 */
	public final int retrytime;

	/**
	 * max time (?!)
	 */
	public final int maxtime;

	/**
	 * @param db
	 */
	TransferDetails(final DBFunctions db) {
		transferId = db.geti("transferId");
		priority = db.geti("priority");
		ctime = db.getDate("ctime");
		status = StringFactory.get(db.gets("status", null));
		jdl = db.gets("jdl", null);
		lfn = db.gets("lfn", null);
		size = db.getl("size");
		error = db.geti("error");
		started = db.getl("started");
		sent = db.getl("sent");
		finished = db.getl("finished");
		received = db.getl("received");
		expires = db.geti("expires");
		transferGroup = db.geti("transferGroup");
		options = StringFactory.get(db.gets("options", null));
		destination = StringFactory.get(db.gets("destination", null));
		user = StringFactory.get(db.gets("user", null));
		attempts = db.geti("attempts");
		protocols = StringFactory.get(db.gets("protocols", null));
		type = StringFactory.get(db.gets("type", null));
		agentid = db.geti("agentid");
		reason = db.gets("reason", null);
		pfn = db.gets("pfn", null);
		protocolid = StringFactory.get(db.gets("protocolid", null));
		ftd = StringFactory.get(db.gets("ftd", null));
		persevere = db.geti("persevere");
		retrytime = db.geti("retrytime");
		maxtime = db.geti("maxtime");
	}

}
