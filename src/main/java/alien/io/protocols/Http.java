/**
 *
 */
package alien.io.protocols;

import java.io.File;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

import alien.catalogue.PFN;
import alien.config.ConfigUtils;
import alien.io.IOUtils;

/**
 * @author costing
 * @since Dec 8, 2010
 */
public class Http extends Protocol {
	/**
	 *
	 */
	private static final long serialVersionUID = -9087355732313314671L;
	/**
	 * Logger
	 */
	static transient final Logger logger = ConfigUtils.getLogger(Http.class.getCanonicalName());

	/**
	 * package protected
	 */
	Http() {
		// package protected
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see alien.io.protocols.Protocol#get(alien.catalogue.PFN, alien.catalogue.access.CatalogueReadAccess, java.lang.String)
	 */
	@Override
	public File get(final PFN pfn, final File localFile) throws IOException {
		File target = null;

		if (localFile != null) {
			target = localFile;

			if (!target.createNewFile())
				throw new IOException("Local file " + localFile + " could not be created");
		}

		if (target == null) {
			// we are free to use any cached value
			target = TempFileManager.getAny(pfn.getGuid());

			if (target != null) {
				logger.log(Level.FINE, "Reusing cached file: " + target.getCanonicalPath());

				return target;
			}

			target = File.createTempFile("http", null, IOUtils.getTemporaryDirectory());
		}

		try {
			lazyj.Utils.download(pfn.pfn, target.getCanonicalPath());

			if (!checkDownloadedFile(target, pfn))
				throw new IOException("Local file doesn't match catalogue details");

			if (localFile == null)
				TempFileManager.putTemp(pfn.getGuid(), target);
			else
				TempFileManager.putPersistent(pfn.getGuid(), target);
		}
		catch (final IOException ioe) {
			if (!target.delete())
				logger.log(Level.WARNING, "Could not delete temporary file on exception : " + target);

			throw ioe;
		}

		return target;
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "http";
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see alien.io.protocols.Protocol#delete(alien.catalogue.PFN)
	 */
	@Override
	public boolean delete(final PFN pfn) throws IOException {
		throw new UnsupportedOperationException();
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see alien.io.protocols.Protocol#put(alien.catalogue.PFN, java.io.File)
	 */
	@Override
	public String put(final PFN pfn, final File localFile) throws IOException {
		throw new UnsupportedOperationException();
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see alien.io.protocols.Protocol#transfer(alien.catalogue.PFN, alien.catalogue.PFN)
	 */
	@Override
	public String transfer(final PFN source, final PFN target) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	int getPreference() {
		return 11;
	}

	@Override
	public boolean isSupported() {
		return true;
	}

	@Override
	public byte protocolID() {
		return 1;
	}
}
