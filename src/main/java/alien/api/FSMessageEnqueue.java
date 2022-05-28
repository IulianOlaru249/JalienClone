package alien.api;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

/**
 * Enqueue messages in the output directory
 *
 * @author costing
 * @since 2015-10-14
 */
public class FSMessageEnqueue {

	private final File outbox;
	private final File inbox;

	/**
	 * Check if directory exists (or try to create it if not) and can be written to
	 *
	 * @param dir
	 * @throws IOException
	 */
	static void checkDir(final File dir) throws IOException {
		if (!dir.exists())
			if (!dir.mkdirs())
				throw new IOException("Directory " + dir.getAbsolutePath() + " could not be created");

		if (!dir.isDirectory())
			throw new IOException("Path " + dir.getAbsolutePath() + " exists and is not a directory");

		if (!dir.canRead() || !dir.canWrite())
			throw new IOException("Directory " + dir.getAbsolutePath() + " is not R/W");
	}

	/**
	 * Create a message queue
	 *
	 * @param outboxPath
	 *            path where the outgoing messages will be written. If it doesn't exist, it will try to create this directory.
	 * @param inboxPath
	 *            path where to check for replies. If <code>null</code> the same directory as for outgoing messages will be used. Same as for outbox, it will try to create the directory if not
	 *            previously existing.
	 * @throws IOException
	 *             in case it cannot create the directory or if the path exists but is not a directory, or is not writable...
	 */
	public FSMessageEnqueue(final String outboxPath, final String inboxPath) throws IOException {
		this.outbox = new File(outboxPath);

		checkDir(this.outbox);

		if (inboxPath == null)
			this.inbox = this.outbox;
		else {
			this.inbox = new File(inboxPath);

			checkDir(this.inbox);
		}
	}

	private long sleepTime = 500;

	/**
	 * Change the poll interval for the reply file to show up
	 *
	 * @param millis
	 * @return the previous value of this field
	 */
	public long sleepBetweenChecks(final long millis) {
		final long oldValue = sleepTime;

		this.sleepTime = millis;

		return oldValue;
	}

	private long timeout = 1000 * 60 * 10;

	/**
	 * Set the timeout for waiting for a reply
	 *
	 * @param millis
	 *            new timeout value in milliseconds
	 * @return the previous timeout value
	 */
	public long setTimeout(final long millis) {
		final long oldValue = timeout;

		this.timeout = millis;

		return oldValue;
	}

	/**
	 * Send a request upstream and wait for the reply (unless the Request implements {@link OneWayMessage} in which case it quickly retuns <code>null</code>.
	 *
	 * @param r
	 *            Request to send upstream
	 * @return The answer to this Request, an object of the same type filled with the answer to the request, or <code>null</code> if the parameter implements {@link OneWayMessage}.
	 * @throws IOException
	 * @throws ServerException
	 */
	public <T extends Request> T dispatchRequest(final T r) throws IOException, ServerException {
		if (r == null)
			return null;

		final String filename = r.getVMUUID().toString() + "." + r.getRequestID();

		final File fOut = new File(outbox, filename + ".out");

		try (ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(fOut))) {
			oos.writeObject(r);
		}

		if (r instanceof OneWayMessage)
			return null;

		// and wait for the reply
		final File fIn = new File(inbox, filename + ".in");

		final long lStart = System.currentTimeMillis();

		while ((System.currentTimeMillis() - lStart < timeout) && (!fIn.exists() || !fIn.canRead()))
			try {
				Thread.sleep(sleepTime);
			}
			catch (final InterruptedException ie) {
				throw new IOException("Interrupted wait", ie);
			}

		if (!fIn.exists() || !fIn.canRead())
			throw new IOException("Timeout waiting for a reply");

		Object o;
		try (ObjectInputStream ois = new ObjectInputStream(new FileInputStream(fIn))) {
			o = ois.readObject();
		}
		catch (final ClassNotFoundException e) {
			throw new IOException(e.getMessage());
		}
		finally {
			fIn.delete();
		}

		@SuppressWarnings("unchecked")
		final T reply = (T) o;

		final ServerException ex = reply.getException();

		if (ex != null)
			throw ex;

		return reply;
	}
}
