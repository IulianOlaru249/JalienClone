package alien.taskQueue;

import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.SignatureException;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicLong;

import java.security.cert.X509Certificate;

import alien.user.AliEnPrincipal;
import alien.user.JAKeyStore;
import lazyj.Utils;

/**
 * @author ron
 * @since Jun 05, 2011
 */
public class TaskQueueFakeUtils {

	private static AtomicLong jobcounter = new AtomicLong(System.currentTimeMillis() / 1000L);

	private static HashMap<Long, Job> queue = new HashMap<>();

	/**
	 * @return a job
	 */
	public static synchronized Job getJob() {
		final Long currentJobID = Long.valueOf(jobcounter.longValue());

		final Job j = queue.get(currentJobID);
		if (j != null && j.status() == JobStatus.WAITING) {
			System.out.println("submitting job: " + j.jdl);
			setJobStatus(j.queueId, JobStatus.ASSIGNED);
			return j;
		}

		return null;
	}

	/**
	 * @return fake job
	 */
	public static Job fakeJob() {
		final Job j = new Job();
		j.queueId = jobcounter.incrementAndGet();

		// j.status = "WAITING";

		j.jdl = Utils.readFile("/tmp/myFirst.jdl");

		j.site = "";
		j.started = 0;
		queue.put(Long.valueOf(j.queueId), j);
		return j;
	}

	/**
	 * Submit a job
	 * 
	 * @param jdl
	 * @param user
	 * @param cert
	 * @return job id
	 * @throws JobSubmissionException
	 */
	public static int submitJob(final String jdl, final AliEnPrincipal user, final X509Certificate[] cert) throws JobSubmissionException {

		try {
			if (JobSigner.verifyJob(cert, user, jdl)) {

				final Job j = new Job();
				j.queueId = jobcounter.incrementAndGet();
				System.out.println("Assigning jobID: " + j.queueId);

				// j.status = "WAITING";
				j.userCertificate = cert[0];

				JDL ojdl = null;
				try {
					System.out.println("creating JDL with String:" + jdl);
					ojdl = new JDL(jdl);
				}
				catch (final IOException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}

				j.jdl = JobSigner.signJob(JAKeyStore.getKeyStore(), "User.cert", JAKeyStore.pass, user.getName(), ojdl).toString();

				j.site = "";
				j.started = 0;

				queue.put(Long.valueOf(j.queueId), j);

				System.out.println("We put the job in the QUEUE: " + j.jdl);

				return jobcounter.intValue();
			}
		}
		catch (final InvalidKeyException e) {
			e.printStackTrace();
			System.out.println("InvalidKeyException");
		}
		catch (final NoSuchAlgorithmException e) {
			e.printStackTrace();
			System.out.println("NoSuchAlgorithmException");
		}
		catch (final SignatureException e) {
			e.printStackTrace();
			System.out.println("SignatureException");
		}
		catch (final KeyStoreException e) {
			e.printStackTrace();
			System.out.println("KeyStoreException");
		}
		System.out.println("Job dismissed");

		return -1;

	}

	/**
	 * 
	 * @param jobID
	 * @param status
	 */
	public static void setJobStatus(final long jobID, final JobStatus status) {
		// queue.get(jobID).status = status;
		System.out.println("Setting job [" + jobID + "] to status <" + status + ">");
	}

	/**
	 * @param jobID
	 * @return the status
	 */
	public static JobStatus getJobStatus(@SuppressWarnings("unused") final long jobID) {
		// if (jobID != 0 && queue.containsKey(jobID))
		// if (queue.get(jobID) != null)
		// ignore
		// return queue.get(jobID).status;
		return null;
	}

}
