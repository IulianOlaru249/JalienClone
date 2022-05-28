package utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipInputStream;

import alien.catalogue.GUID;
import alien.catalogue.GUIDUtils;
import alien.catalogue.LFN;
import alien.catalogue.LFNUtils;
import alien.catalogue.PFN;
import alien.catalogue.access.AccessType;
import alien.catalogue.access.AuthorizationFactory;
import alien.config.ConfigUtils;
import alien.io.IOUtils;
import alien.io.protocols.TempFileManager;
import alien.io.protocols.XRDStatus;
import alien.io.protocols.Xrootd;
import alien.se.SE;
import alien.se.SEUtils;
import lazyj.Format;

/**
 * @author costing
 *
 */
public class XRDChecker {

	/**
	 * Logger
	 */
	static final Logger logger = ConfigUtils.getLogger(XRDChecker.class.getCanonicalName());

	/**
	 * @param guid
	 * @return the status for each PFN of this LFN (the real ones)
	 */
	public static final Map<PFN, XRDStatus> check(final GUID guid) {
		if (guid == null)
			return null;

		final Set<GUID> realGUIDs = guid.getRealGUIDs();

		if (realGUIDs == null || realGUIDs.size() == 0)
			return null;

		final Map<PFN, XRDStatus> ret = new HashMap<>();

		final Xrootd xrootd = new Xrootd();

		for (final GUID realId : realGUIDs) {
			final Set<PFN> pfns = realId.getPFNs();

			if (pfns == null)
				continue;

			for (final PFN pfn : pfns) {
				final String reason = AuthorizationFactory.fillAccess(pfn, AccessType.READ);

				if (reason != null) {
					ret.put(pfn, new XRDStatus(false, reason));
					continue;
				}

				try {
					final String output = xrootd.xrdstat(pfn, false, false, false);

					ret.put(pfn, new XRDStatus(true, output));
				}
				catch (final IOException ioe) {
					ret.put(pfn, new XRDStatus(false, ioe.getMessage()));

					if (logger.isLoggable(Level.FINE))
						logger.log(Level.FINE, "Replica is not ok: " + pfn.pfn, ioe);
				}
			}
		}

		return ret;
	}

	/**
	 * @param lfn
	 * @return the status for each PFN of this LFN (the real ones)
	 */
	public static final Map<PFN, XRDStatus> check(final LFN lfn) {
		if (lfn == null)
			return null;

		final GUID guid = GUIDUtils.getGUID(lfn);

		if (guid == null)
			return null;

		return check(guid);
	}

	/**
	 * @param pfn
	 * @return the check status
	 */
	public static final XRDStatus checkByDownloading(final PFN pfn) {
		return checkByDownloading(pfn, false);
	}

	/**
	 * @param pfn
	 * @param zipFile
	 *            if <code>true</code> then the downloaded file is assumed to be a ZIP archive and the code will check its structure as well
	 * @return the check status
	 */
	public static final XRDStatus checkByDownloading(final PFN pfn, final boolean zipFile) {
		final Xrootd xrootd = new Xrootd();

		xrootd.setTimeout(60);

		File f = null;

		final GUID guid = pfn.getGuid();

		try {
			f = File.createTempFile("xrdstatus-", "-download.tmp", IOUtils.getTemporaryDirectory());

			if (!f.delete())
				return new XRDStatus(false, "Could not delete the temporary created file, xrdcp would fail, bailing out");

			if (pfn.ticket == null) {
				final String reason = AuthorizationFactory.fillAccess(pfn, AccessType.READ);

				if (reason != null)
					return new XRDStatus(false, reason);
			}
			final long lStart = System.currentTimeMillis();

			System.err.println("Getting this file " + pfn.pfn);

			xrootd.get(pfn, f);

			System.err.println("Got the file in " + Format.toInterval(System.currentTimeMillis() - lStart));

			if (f.length() != guid.size)
				return new XRDStatus(false, "Size is different: catalog=" + guid.size + ", downloaded size: " + f.length());

			if (guid.md5 != null && guid.md5.length() > 0) {
				final String fileMD5 = IOUtils.getMD5(f);

				if (!fileMD5.equalsIgnoreCase(guid.md5))
					return new XRDStatus(false, "MD5 is different: catalog=" + guid.md5 + ", downloaded file=" + fileMD5);
			}

			if (zipFile) {
				final String zipMessage = checkZipFile(f);

				if (zipMessage != null)
					return new XRDStatus(false, "Broken ZIP archive: " + zipMessage);
			}
		}
		catch (final IOException ioe) {
			return new XRDStatus(false, ioe.getMessage());
		}
		finally {
			if (f != null) {
				TempFileManager.release(f);

				if (!f.delete())
					System.err.println("Could not delete: " + f);
			}
		}

		return new XRDStatus(true, null);
	}

	/**
	 * Check the integrity of a local ZIP file
	 *
	 * @param f
	 * @return if everything is OK then the method returns <code>null</code>, otherwise it is the error message from the check
	 */
	public static final String checkZipFile(final File f) {
		try (ZipFile zipfile = new ZipFile(f); ZipInputStream zis = new ZipInputStream(new FileInputStream(f))) {
			ZipEntry ze = zis.getNextEntry();
			if (ze == null)
				return "No entry found";
			while (ze != null) {
				// if it throws an exception fetching any of the following then we know the file is corrupted.
				try (InputStream is = zipfile.getInputStream(ze)) {
					ze.getCrc();
					ze.getCompressedSize();
					// System.err.println("All ok with " + ze.getName());
				}
				ze = zis.getNextEntry();
			}
			return null;
		}
		catch (final IOException e) {
			return e.getMessage();
		}
	}

	private static SE noSE = SEUtils.getSE("no_se");

	/**
	 * Check all replicas of an LFN, first just remotely querying the status then fully downloading each replica and computing the md5sum.
	 *
	 * @param sLFN
	 * @return the status of all replicas
	 */
	public static final Map<PFN, XRDStatus> fullCheckLFN(final String sLFN) {
		final GUID guid;

		final boolean zipArchive;

		if (GUIDUtils.isValidGUID(sLFN)) {
			guid = GUIDUtils.getGUID(sLFN);

			zipArchive = false;
		}
		else {
			final LFN lfn = LFNUtils.getLFN(sLFN);

			if (lfn == null)
				return null;

			guid = GUIDUtils.getGUID(lfn);

			zipArchive = sLFN.toLowerCase().endsWith(".zip") || sLFN.substring(sLFN.lastIndexOf('/')).toLowerCase().contains("archive") || guid.hasReplica(noSE);
		}

		if (guid == null)
			return null;

		final Map<PFN, XRDStatus> check = XRDChecker.check(guid);

		if (check == null || check.size() == 0)
			return check;

		final Iterator<Map.Entry<PFN, XRDStatus>> it = check.entrySet().iterator();

		while (it.hasNext()) {
			final Map.Entry<PFN, XRDStatus> entry = it.next();

			final PFN pfn = entry.getKey();

			final XRDStatus status = entry.getValue();

			if (status.commandOK) {
				final XRDStatus downloadStatus = checkByDownloading(pfn, zipArchive);

				if (!downloadStatus.commandOK)
					entry.setValue(downloadStatus);
			}
		}

		return check;
	}

}
