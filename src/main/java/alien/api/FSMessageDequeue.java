package alien.api;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.logging.Level;
import java.util.logging.Logger;

import alien.api.catalogue.SEfromString;
import alien.config.ConfigUtils;
import lazyj.Format;

/**
 * Dequeue request objects and send back replies
 *
 * @author costing
 * @since 2015-10-14
 */
public class FSMessageDequeue {

	/**
	 * Logger
	 */
	static final Logger logger = ConfigUtils.getLogger(FSMessageDequeue.class.getCanonicalName());

	private final File inboxFolder;

	private final String outboxFolderPattern;

	/**
	 * Create a dequeueing object passing the inbox path and the outbox pattern.
	 *
	 * @param inbox
	 *            inbox directory name. The code will try to create the directory if the path doesn't exist and will throw an exception if there is any problem related to permissions and such.
	 * @param outboxPattern
	 *            outbox path pattern. This will not be checked initially but only when sending back a reply. The following constructs will be replaced:
	 *            <ul>
	 *            <li><b><code>{UUID}</code></b> will be replaced with the {@link Request} VM UUID ({@link Request#getVMUUID()})</li>
	 *            <li><b><code>{REQID}</code></b> with the {@link Request} sequence ID ({@link Request#getRequestID()})</li>
	 *            </ul>
	 * @throws IOException
	 */
	public FSMessageDequeue(final String inbox, final String outboxPattern) throws IOException {
		this.inboxFolder = new File(inbox);

		FSMessageEnqueue.checkDir(inboxFolder);

		this.outboxFolderPattern = outboxPattern;
	}

	private long sleepTime = 500;

	/**
	 * Set how long to sleep between polls to the input box
	 *
	 * @param millis
	 * @return the old value
	 */
	public long setSleepTime(final long millis) {
		final long oldValue = millis;

		this.sleepTime = millis;

		return oldValue;
	}

	private final LinkedList<File> previousListing = new LinkedList<>();

	private void refreshListing() {
		if (previousListing.size() > 0)
			return;

		final File[] list = inboxFolder.listFiles();

		if (list != null && list.length > 0)
			for (final File f : list)
				if (f.isFile() && f.canRead() && f.getName().endsWith(".out"))
					previousListing.add(f);
	}

	/**
	 * Read the next request from the inbox queue
	 *
	 * @param timeout
	 *            how long to wait for a file to show up. <code>0</code> means one-shot checking, a negative value means forever while a positive value is a timeout in milliseconds
	 * @return the next {@link Request} object, or <code>null</code> if nothing showed up in the specified time
	 */
	public synchronized Request readNextRequest(final int timeout) {
		final long lStart = System.currentTimeMillis();

		do {
			refreshListing();

			final Iterator<File> it = previousListing.iterator();

			while (it.hasNext()) {
				final File f = it.next();

				try (ObjectInputStream ois = new ObjectInputStream(new FileInputStream(f))) {
					final Object o = ois.readObject();

					if (o == null || !(o instanceof Request))
						throw new IOException("Invalid content of " + f.getAbsolutePath());

					f.delete();
					it.remove();

					return (Request) o;
				}
				catch (ClassNotFoundException | IOException e) {
					logger.log(Level.WARNING, "Exception processing " + f.getAbsolutePath(), e);
				}
			}

			if (timeout != 0)
				try {
					Thread.sleep(sleepTime);
				}
				catch (@SuppressWarnings("unused") final InterruptedException ie) {
					return null;
				}
		} while (timeout < 0 || (timeout > 0 && (System.currentTimeMillis() - lStart < timeout)));

		return null;
	}

	/**
	 * Enqueue a reply in the outbox folder constructed by combining the outbox pattern with the {@link Request} object details
	 *
	 * @param r
	 *            object to send back
	 * @throws IOException
	 *             if there is any error sending the reply
	 * @see #FSMessageDequeue(String, String)
	 */
	public void enqueueReply(final Request r) throws IOException {
		String path = Format.replace(outboxFolderPattern, "{UUID}", r.getVMUUID().toString());
		path = Format.replace(path, "{REQID}", r.getRequestID().toString());

		final File fOutbox = new File(path);

		// throws an exception if the folder has any apparent problems
		FSMessageEnqueue.checkDir(fOutbox);

		final File fOut = new File(fOutbox, r.getVMUUID().toString() + "." + r.getRequestID() + ".in");

		try (ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(fOut))) {
			oos.writeObject(r);
		}
	}

	/**
	 * testing method
	 *
	 * @param args
	 * @throws InterruptedException
	 */
	public static void main(final String[] args) throws InterruptedException {
		final Thread tOut = new Thread() {
			@Override
			public void run() {
				try {
					final FSMessageEnqueue q = new FSMessageEnqueue("/tmp/qout", "/tmp/qin/" + Request.getVMID());

					final Request r = new SEfromString(null, "ALICE::CERN::EOS");

					final Request reply = q.dispatchRequest(r);

					System.err.println("And got back:\n" + reply);
				}
				catch (final Throwable t) {
					System.err.println(t);
					t.printStackTrace();
				}
			}
		};

		final Thread tIn = new Thread() {
			@Override
			public void run() {
				try {
					final FSMessageDequeue q = new FSMessageDequeue("/tmp/qout", "/tmp/qin/{UUID}");

					final Request r = q.readNextRequest(-1);

					final Request reply = Dispatcher.execute(r);

					q.enqueueReply(reply);

				}
				catch (final Throwable t) {
					System.err.println(t);
					t.printStackTrace();
				}
			}
		};

		tOut.start();
		tIn.start();

		tOut.join();
		tIn.join();
	}
}
