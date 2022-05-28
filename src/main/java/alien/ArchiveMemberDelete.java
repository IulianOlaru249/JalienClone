package alien;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import alien.api.Dispatcher;
import alien.api.ServerException;
import alien.api.catalogue.CatalogueApiUtils;
import alien.api.catalogue.PFNforReadOrDel;
import alien.catalogue.LFN;
import alien.catalogue.PFN;
import alien.catalogue.XmlCollection;
import alien.catalogue.access.AccessType;
import alien.config.ConfigUtils;
import alien.io.protocols.Factory;
import alien.shell.commands.JAliEnCOMMander;
import alien.site.OutputEntry;
import joptsimple.OptionParser;
import joptsimple.OptionSet;

/**
 * @author vyurchen
 *
 */
public class ArchiveMemberDelete {

	private static JAliEnCOMMander commander = null;
	private final static String usrdir = System.getProperty("user.dir");
	private final static String separator = System.getProperty("file.separator");

	/**
	 * @param args
	 * @throws Exception
	 */
	public static void main(final String[] args) throws Exception {

		if (args.length > 0) {
			try {
				commander = JAliEnCOMMander.getInstance();
			}
			catch (final ExceptionInInitializerError | NullPointerException e) {
				System.err.println("Failed to get a JAliEnCOMMander instance. Abort");
				e.printStackTrace();
				return;
			}

			final OptionParser parser = new OptionParser();
			parser.accepts("list").withRequiredArg(); // Like "collection.xml"
			parser.accepts("purge");
			final OptionSet options = parser.parse(args);
			final boolean purge = options.has("purge");

			// Read archive members names from file
			final String collectionName = (String) options.valueOf("list");

			final File collectionFile = new File(collectionName);
			if (!collectionFile.exists()) {
				System.err.println("Couldn't open the collection! File " + collectionName + " doesn't exist");
				return;
			}
			final XmlCollection xmlCollection = new XmlCollection(collectionFile);

			Iterator<LFN> xmlEntries = xmlCollection.iterator();
			System.out.println("We will process next files:");
			while (xmlEntries.hasNext()) {
				System.out.println("- " + xmlEntries.next().getCanonicalName());
			}
			System.out.println();

			xmlEntries = xmlCollection.iterator();
			while (xmlEntries.hasNext()) {
				deleteArchiveMember(xmlEntries.next().getCanonicalName(), purge);
			}
			System.out.println();
			System.out.println("All files processed. Exiting");

			final Path validation = Path.of("validation_error.message");
			if (Files.exists(validation) && Files.size(validation) == 0)
				cleanUpLocal(validation);
		}
	}

	private static void deleteArchiveMember(final String xmlEntry, final boolean purge) {
		// Use this for debugging
		// final ByteArrayOutputStream out = new ByteArrayOutputStream();
		// commander.setLine(new JSONPrintWriter(out), null);
		System.out.println();
		System.out.println("[" + new Date() + "] Processing " + xmlEntry);

		// Parse the parent directory from the entry, e.g.:
		// /alice/sim/2018/LHC18e1a/246053/075/BKG/TrackRefs.root ->
		// /alice/sim/2018/LHC18e1a/246053/075
		String parentdir = xmlEntry.substring(0, xmlEntry.lastIndexOf('/'));
		final String lastStringToken = parentdir.substring(parentdir.lastIndexOf('/') + 1);
		if (!lastStringToken.matches("^\\d+.\\d+$")) {
			parentdir = parentdir.substring(0, parentdir.lastIndexOf('/'));
		}

		final String registerPath = parentdir + "/registertemp";

		final LFN remoteLFN;
		try {
			remoteLFN = commander.c_api.getLFN(xmlEntry);
		}
		catch (final NullPointerException e) {
			// Connection may be refused
			System.err.println("[" + new Date() + "] Something went wrong. Abort.");
			e.printStackTrace();
			return;
		}

		// Clean up previous iterations
		//
		if (!cleanUpRegistertemp(xmlEntry, registerPath, remoteLFN, parentdir))
			return;

		// Continue basic checks
		// Check if we are able to get PFN list
		List<PFN> remotePFN = null;
		try {
			remotePFN = Dispatcher.execute(new PFNforReadOrDel(commander.getUser(), commander.getSite(), AccessType.DELETE, remoteLFN, null, null)).getPFNs();
		}
		catch (final ServerException e1) {
			System.err.println("[" + new Date() + "] " + xmlEntry + ": Could not get PFN. Abort");
			e1.printStackTrace();
			return;
		}

		// If not - the file is orphaned
		if (remotePFN == null || remotePFN.isEmpty()) {
			System.err.println("[" + new Date() + "] " + xmlEntry + ": Can't get PFNs for this file. Abort");
			return;
		}

		final String remoteFile = remoteLFN.getCanonicalName();
		final long remoteFileSize = remoteLFN.getSize();

		final LFN remoteArchiveLFN = commander.c_api.getRealLFN(remoteFile);
		if (remoteArchiveLFN == null || !remoteArchiveLFN.exists) {
			System.err.println("[" + new Date() + "] " + remoteFile + ": Archive not found in parent dir. Abort");
			return;
		}

		// If the file is not a member of any archives, just delete it
		if (remoteLFN.equals(remoteArchiveLFN)) {
			System.out.println("[" + new Date() + "] " + remoteFile + " is a real file, we'll simply delete it");

			// Speed up things by calling xrootd delete directly
			xrdDeleteRemoteFile(remoteFile, remotePFN);

			commander.c_api.removeLFN(remoteFile);

			System.out.println("[" + new Date() + "] " + remoteFile);
			System.out.println("[" + new Date() + "] Reclaimed " + remoteFileSize + " bytes of disk space");
			return;
		}

		// Main procedure
		//
		try (PrintWriter validation = new PrintWriter(new FileOutputStream("validation_error.message", true))) {

			List<PFN> remoteArchivePFN = null;
			if (purge) {
				remoteArchivePFN = Dispatcher.execute(new PFNforReadOrDel(commander.getUser(), commander.getSite(), AccessType.DELETE, remoteArchiveLFN, null, null)).getPFNs();
				if (remoteArchivePFN.isEmpty()) {
					System.err.println("[" + new Date() + "] " + remoteFile + ": Archive is orphaned");
					validation.println("Orphaned archive " + remoteFile);
					return;
				}
			}

			final String remoteArchive = remoteArchiveLFN.getCanonicalName();
			final String archiveName = remoteArchiveLFN.getFileName();
			final String memberName = remoteLFN.getFileName();
			final long jobID = ConfigUtils.getConfig().getl("ALIEN_PROC_ID", remoteArchiveLFN.jobid);
			final long remoteArchiveSize = remoteArchiveLFN.getSize();

			final List<LFN> remoteArchiveMembers = commander.c_api.getArchiveMembers(remoteArchive);
			if (remoteArchiveMembers == null || remoteArchiveMembers.isEmpty()) {
				System.err.println("[" + new Date() + "] Failed to get members of the remote archive: " + remoteArchive);
				validation.println("Failed to get members of " + remoteArchive);
				return;
			}
			if (remoteArchiveMembers.size() == 1) {
				// RemoteLFN is the only file in remoteArchive
				// No point in downloading it, just remove file and archive

				System.out.println("[" + new Date() + "] Deleting remote file");
				commander.c_api.removeLFN(remoteFile);

				System.out.println("[" + new Date() + "] Deleting old remote archive");

				// Remove physical replicas of the old archive
				xrdDeleteRemoteFile(remoteFile, remoteArchivePFN);

				// Remove lfn of the old archive
				commander.c_api.removeLFN(remoteArchive);

				System.out.println("[" + new Date() + "] " + memberName + " was " + remoteFileSize + " bytes");
				System.out.println("[" + new Date() + "] " + "Old archive was " + remoteArchiveSize + " bytes");
				System.out.println("[" + new Date() + "] " + "Reclaimed " + remoteArchiveSize + " bytes of disk space");

				return;
			}

			final File localArchive = new File(usrdir + separator + archiveName);
			if (Files.exists(localArchive.toPath()))
				cleanUpLocal(localArchive.toPath());

			// Download the archive from the Grid
			//
			System.out.println("[" + new Date() + "] Downloading the archive from the Grid");
			commander.c_api.downloadFile(remoteArchive, localArchive, "-silent");
			if (!Files.exists(localArchive.toPath())) {
				System.err.println("[" + new Date() + "] " + remoteFile + ": Failed to download remote archive " + remoteArchive);
				validation.println("Download failed " + remoteArchive);
				return;
			}

			// Unpack to local directory and zip again without member file
			//
			System.out.println("[" + new Date() + "] Unpacking to local directory");
			if (!unzip(archiveName, memberName)) {
				System.err.println("[" + new Date() + "] " + remoteFile + ": Failed to extract files from archive: " + usrdir + separator + archiveName);
				validation.println("Extraction failed " + remoteArchive);
				cleanUpLocal(localArchive.toPath());
				cleanUpLocal(Path.of(usrdir, "extracted"));
				return;
			}
			cleanUpLocal(localArchive.toPath());

			final ArrayList<String> listOfFiles = getFileListing(Path.of(usrdir, "extracted"));

			System.out.println("[" + new Date() + "] Zipping the new archive");
			final OutputEntry entry = new OutputEntry(archiveName, listOfFiles, "", Long.valueOf(jobID), true);
			entry.createZip(usrdir + separator + "extracted");

			final String newArchiveFullPath = registerPath + separator + archiveName;
			final File newArchive = new File(usrdir + separator + "extracted" + separator + archiveName);
			final long newArchiveSize = newArchive.length();

			if (newArchiveSize == 0) {
				System.err.println("[" + new Date() + "] " + remoteFile + ": Produced archive is 0 bytes");
				validation.println("Zipping failed  " + newArchiveFullPath);
				cleanUpLocal(Path.of(usrdir, "extracted"));
				return;
			}

			// Upload the new archive to the Grid
			//
			while (commander.c_api.getLFN(newArchiveFullPath) != null) {
				// Delete registertemp/root_archive.zip if there is any
				System.out.println("[" + new Date() + "] Deleting corrupted " + newArchiveFullPath);
				commander.c_api.removeLFN(newArchiveFullPath);
			}

			System.out.println("[" + new Date() + "] Uploading the new archive to the Grid: " + newArchiveFullPath);
			// Create only one replica
			if (commander.c_api.uploadFile(newArchive, newArchiveFullPath, "-w", "-S", "disk:1") == null) {
				System.err.println("[" + new Date() + "] " + remoteFile + ": Failed to upload archive " + newArchiveFullPath);
				validation.println("Upload failed " + newArchiveFullPath);
				cleanUpLocal(Path.of(usrdir, "extracted"));
				if (registerPath.length() > 20) // Safety check
					commander.c_api.removeLFN(registerPath, true);
				return;
			}

			final LFN newArchiveLFN = commander.c_api.getLFN(newArchiveFullPath);
			if (newArchiveLFN == null || !newArchiveLFN.exists) {
				System.err.println("[" + new Date() + "] " + remoteFile + ": Couldn't find archive " + newArchiveFullPath);
				validation.println("Couldn't find  " + newArchiveFullPath);
				cleanUpLocal(Path.of(usrdir, "extracted"));
				if (registerPath.length() > 20) // Safety check
					commander.c_api.removeLFN(registerPath, true);
				return;
			}

			if (newArchiveLFN.getSize() == 0) {
				System.err.println("[" + new Date() + "] " + remoteFile + ": Uploaded archive is 0 bytes: " + newArchiveFullPath);
				validation.println("Upload failed  " + newArchiveFullPath);
				cleanUpLocal(Path.of(usrdir, "extracted"));
				if (registerPath.length() > 20) // Safety check
					commander.c_api.removeLFN(registerPath, true);
				return;
			}

			// Clean up local files
			cleanUpLocal(Path.of(usrdir, "extracted"));

			// Register archive members in the catalogue
			//
			if (!registerFiles(entry, registerPath, remoteFile, validation))
				return;

			// Delete old files
			//
			if (!deleteRemoteArchive(remoteFile, remoteArchive, remoteArchiveMembers, registerPath, remoteArchivePFN, validation))
				return;

			// Move new files from registertemp to parentdir
			//
			if (!renameFiles(remoteArchive, listOfFiles, archiveName, registerPath, remoteFile, validation, parentdir))
				return;

			// Clean up
			if (registerPath.length() > 20) // Safety check
				commander.c_api.removeLFN(registerPath, true);

			System.out.println("[" + new Date() + "] " + memberName + " was " + remoteFileSize + " bytes");
			System.out.println("[" + new Date() + "] " + "Old archive was " + remoteArchiveSize + " bytes");
			System.out.println("[" + new Date() + "] " + "New archive is " + newArchiveSize + " bytes");
			System.out.println("[" + new Date() + "] " + "Reclaimed " + (remoteArchiveSize - newArchiveSize) + " bytes of disk space");
		}
		catch (final IOException e1) {
			System.err.println("[" + new Date() + "] " + remoteFile + ": I/O exception. Abort");
			e1.printStackTrace();
		}
		catch (final ServerException e1) {
			System.err.println("[" + new Date() + "] " + remoteFile + ": Could not get PFN. Abort");
			e1.printStackTrace();
		}
		catch (final OutOfMemoryError e1) {
			System.err.println("[" + new Date() + "] " + "Out of memory. Abort");
			e1.printStackTrace();
		}
	}

	/**
	 * Size of the buffer to read/write data
	 */
	private static final int BUFFER_SIZE = 4096;

	/**
	 * Extracts a zip file specified by the zipFilePath to a directory specified by
	 * destDirectory (will be created if does not exists)
	 *
	 * @param jobID
	 * @throws IOException
	 */
	private static boolean unzip(final String archiveName, final String memberName) throws IOException {

		final Path destDir = Path.of(usrdir, "extracted");
		if (!Files.exists(destDir)) {
			Files.createDirectories(destDir);
		}
		else {
			// Clean up temp directory if there are files in it
			try (Stream<Path> destDirEntries = Files.list(destDir)) {
				destDirEntries.forEach(ArchiveMemberDelete::cleanUpLocal);
			}
		}

		// Start unpacking the archive
		try (ZipInputStream zipIn = new ZipInputStream(new FileInputStream(usrdir + separator + archiveName))) {
			ZipEntry entry = zipIn.getNextEntry();
			// iterates over entries in the zip file
			while (entry != null) {
				if (entry.getName().contains(memberName) || entry.getName().contains("AliESDfriends.root")) {
					// Skip this file; also skip AliESDfriends.root
					System.out.println("[" + new Date() + "] " + " - skipped " + entry.getName() + " with size " + entry.getSize());
					zipIn.closeEntry();
					entry = zipIn.getNextEntry();
					continue;
				}

				final Path filePath = Path.of(destDir.toString(), entry.getName());
				if (!entry.isDirectory()) {
					// If the entry is a file, extract it
					final Path parentFolder = filePath.getParent();

					if (parentFolder != null)
						Files.createDirectories(parentFolder);

					extractFile(zipIn, filePath.toString());
				}
				else {
					// If the entry is a directory, make the directory
					Files.createDirectories(filePath);
				}
				System.out.println("[" + new Date() + "] - extracted " + entry.getName());
				zipIn.closeEntry();
				entry = zipIn.getNextEntry();
			}

			return true;
		}
		catch (final FileNotFoundException e) {
			e.printStackTrace();
			return false;
		}
	}

	/**
	 * Extracts a zip entry (file entry)
	 *
	 * @param zipIn
	 * @param filePath
	 * @throws IOException
	 */
	private static void extractFile(final ZipInputStream zipIn, final String filePath) throws IOException {
		try (BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(filePath))) {
			final byte[] bytesIn = new byte[BUFFER_SIZE];
			int read = 0;
			while ((read = zipIn.read(bytesIn)) != -1) {
				bos.write(bytesIn, 0, read);
			}
		}
	}

	/**
	 * Get list of files in a directory recursively while saving relative paths
	 *
	 * @param folderName
	 *            folder to look inside
	 */
	private static ArrayList<String> getFileListing(final Path folderName) {
		ArrayList<String> result = new ArrayList<>();
		try (Stream<Path> folder = Files.walk(folderName)) {
			result = folder.map(path -> folderName.relativize(path).toString()).collect(Collectors.toCollection(ArrayList<String>::new));
			// Files.walk always returns parent dir as a first element. After relativize it is transformed to "". Remove it from the result
			result.remove("");

			if (result.isEmpty())
				System.err.println("[" + new Date() + "] Failed to get list of files in local folder: " + folderName.toAbsolutePath());
		}
		catch (final IOException e) {
			e.printStackTrace();
		}
		return result;
	}

	/**
	 * Delete local file using <code>java.nio.Files#delete</code>, catch any <code>IOException</code>
	 *
	 * @param path
	 *            path of the local file or directory to delete
	 */
	private static void cleanUpLocal(final Path path) {
		try (Stream<Path> folder = Files.walk(path)) {
			folder.sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(File::delete);
		}
		catch (final IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Delete any leftovers from the previous iterations. Returns <code>true</code> if it is ok to continue member removal procedure
	 *
	 */
	private static boolean cleanUpRegistertemp(final String xmlEntry, final String registerPath, final LFN remoteLFN, final String parentdir) {
		if (remoteLFN == null || !remoteLFN.exists) {
			// TrackRefs was deleted, registertemp contains valid files
			System.err.println("[" + new Date() + "] " + xmlEntry + ": LFN doesn't exist, checking registertemp");

			if (commander.c_api.getLFN(registerPath) != null) {
				// Move everything from registertemp to parent
				System.out.println("[" + new Date() + "] " + "registertemp found, moving out its content");

				final List<LFN> listing = commander.c_api.getLFNs(registerPath);
				if (listing == null || listing.isEmpty()) {
					System.err.println("[" + new Date() + "] " + "Failed to get directory listing for " + registerPath + ". Abort.");
					return false;
				}

				for (final LFN file : commander.c_api.getLFNs(registerPath)) {
					if (file == null || !file.exists) {
						System.err.println("[" + new Date() + "] " + "Failed to get file details from " + registerPath + ". Abort.");
						return false;
					}

					// Check if there is another copy of the same file in parentdir
					final LFN registerMember = commander.c_api.getLFN(registerPath + "/" + file.getFileName());
					final LFN parentMember = commander.c_api.getLFN(parentdir + "/" + file.getFileName());
					if (parentMember != null) {
						if (parentMember.guid.equals(registerMember.guid))
							commander.c_api.removeLFN(parentMember.getCanonicalName(), false, false);
						else
							commander.c_api.removeLFN(parentMember.getCanonicalName());
					}

					System.out.println("[" + new Date() + "] " + "Moving " + registerPath + "/" + file.getFileName());
					if (commander.c_api.moveLFN(registerPath + "/" + file.getFileName(), parentdir + "/" + file.getFileName()) == null) {
						System.err.println("[" + new Date() + "] " + "Failed to move " + file.getFileName() + ". Abort.");
						return false;
					}
				}

				// Delete registertemp dir since all files are moved
				if (registerPath.length() > 20) // Safety check
					commander.c_api.removeLFN(registerPath, true);
			}
			else {
				final Collection<LFN> root_archive = commander.c_api.find(parentdir + "/", "root_archive.zip", 0);
				if (root_archive == null || root_archive.isEmpty()) {
					System.out.println("[" + new Date() + "] " + "registertemp is not there, original archive and it's members have been removed by someone else. Nothing to do");
				}
				else {
					System.out.println("[" + new Date() + "] " + "registertemp is not there, all DONE");
				}
			}
			return false;
		}
		// Else
		// TrackRefs was not deleted, remove invalid files from registertemp
		if (commander.c_api.getLFN(registerPath) != null) {
			// Delete registertemp dir since it can be corrupted
			if (registerPath.length() > 20) // Safety check
				return commander.c_api.removeLFN(registerPath, true);
		}
		else {
			System.out.println("[" + new Date() + "] " + "registertemp is not there, continue with the main procedure");
		}
		return true;
	}

	/**
	 * Delete an archive and it's member from the catalogue
	 *
	 */
	private static boolean deleteRemoteArchive(final String remoteFile, final String remoteArchive, final List<LFN> remoteArchiveMembers, final String registerPath, final List<PFN> remoteArchivePFN,
			final PrintWriter validation) {
		// Delete the members links of old archive
		//
		System.out.println("[" + new Date() + "] Deleting the members links of old archive");
		for (final LFN member : remoteArchiveMembers) {
			System.out.println("[" + new Date() + "] Deleting " + member.getCanonicalName());
			if (!commander.c_api.removeLFN(member.getCanonicalName())) {
				System.err.println("[" + new Date() + "] " + remoteFile + ": Failed to delete old archive member " + member.getCanonicalName());
				validation.println("Archive member deletion failed " + remoteArchive);

				// Delete all newly created entries and directories
				if (registerPath.length() > 20) // Safety check
					commander.c_api.removeLFN(registerPath, true);
				return false;
			}
		}

		// Delete old remote archive
		//
		System.out.println("[" + new Date() + "] Deleting old remote archive");

		// Remove lfn of the old archive
		if (!commander.c_api.removeLFN(remoteArchive)) {
			System.err.println("[" + new Date() + "] " + remoteFile + ": Failed to delete old archive " + remoteArchive);
			validation.println("Archive deletion failed " + remoteArchive);

			// Delete all newly created entries and directories
			if (registerPath.length() > 20) // Safety check
				commander.c_api.removeLFN(registerPath, true);
			return false;
		}

		// Remove physical replicas of the old archive
		xrdDeleteRemoteFile(remoteFile, remoteArchivePFN);
		return true;
	}

	/**
	 * Physically delete the remote file using xrootd utils
	 *
	 */
	private static void xrdDeleteRemoteFile(final String remoteFile, final List<PFN> remotePFN) {
		if (remotePFN != null && !remotePFN.isEmpty()) {
			final Iterator<PFN> it = remotePFN.iterator();
			while (it.hasNext()) {
				final PFN pfn = it.next();

				if (pfn == null) {
					System.err.println("One of the PFNs of " + remoteFile + " is null");
					continue;
				}

				try {
					System.out.println("[" + new Date() + "] Deleting pfn: " + pfn.pfn);
					if (!Factory.xrootd.delete(pfn)) {
						System.err.println("[" + new Date() + "] " + remoteFile + ": Could not delete " + pfn.pfn);
					}
				}
				catch (final IOException e) {
					e.printStackTrace();
				}
			}
		}
	}

	/**
	 * Create directory structure and register archive and members in the Catalogue. Returns <code>true</code> if it is ok to continue member removal procedure
	 *
	 */
	private static boolean registerFiles(final OutputEntry entry, final String registerPath, final String remoteFile, final PrintWriter validation) {
		// Create subdirs (like BKG/)
		for (final String file : entry.getFilesIncluded()) {
			if (file.contains("/")) {
				if (commander.c_api.createCatalogueDirectory(registerPath + "/" + file.substring(0, file.lastIndexOf('/')), true) == null) {
					System.err.println("[" + new Date() + "] " + remoteFile + ": Failed to create new directory " + registerPath + "/" + file.substring(0, file.lastIndexOf('/')));
					validation.println("Mkdir failed " + registerPath + "/" + file.substring(0, file.lastIndexOf('/')));

					// Delete all newly created entries and directories
					if (registerPath.length() > 20) // Safety check
						commander.c_api.removeLFN(registerPath, true);
					return false;
				}
			}
		}

		System.out.println("[" + new Date() + "] Registering files in the catalogue");
		if (!CatalogueApiUtils.registerEntry(entry, registerPath + "/", commander.getUser())) {
			System.err.println("[" + new Date() + "] " + remoteFile + ": Failed to register archive or it's members " + registerPath + "/" + entry.getName());
			validation.println("Register failed " + registerPath + "/" + entry.getName());

			// Delete all newly created entries and directories
			if (registerPath.length() > 20) // Safety check
				commander.c_api.removeLFN(registerPath, true);
			return false;
		}

		// Loop over newly registered files to make sure they exist
		for (final String file : entry.getFilesIncluded()) {
			final LFN entryLFN = commander.c_api.getLFN(registerPath + "/" + file);
			if (entryLFN == null || !entryLFN.exists) {
				System.err.println("[" + new Date() + "] " + remoteFile + ": Failed to register entry " + registerPath + "/" + file);
				validation.println("Register failed " + registerPath + "/" + file);

				// Delete all newly created entries and directories
				if (registerPath.length() > 20) // Safety check
					commander.c_api.removeLFN(registerPath, true);
				return false;
			}
		}
		return true;
	}

	/**
	 * Move new files to the parent directory. Returns <code>true</code> if it is ok to continue member removal procedure
	 *
	 */
	private static boolean renameFiles(final String remoteArchive, final ArrayList<String> listOfFiles, final String archiveName, final String registerPath, final String remoteFile,
			final PrintWriter validation, final String parentdir) {
		// Rename uploaded archive
		//
		System.out.println("[" + new Date() + "] Renaming uploaded archive");
		if (commander.c_api.moveLFN(registerPath + "/" + archiveName, remoteArchive) == null) {
			System.err.println("[" + new Date() + "] " + remoteFile + ": Failed to rename the archive " + registerPath + "/" + archiveName);

			// Check if there is another copy of the same file in parentdir
			final LFN registerArchive = commander.c_api.getLFN(registerPath + "/" + archiveName);
			final LFN parentArchive = commander.c_api.getLFN(remoteArchive);
			if (parentArchive != null && parentArchive.guid.equals(registerArchive.guid)) {
				commander.c_api.removeLFN(registerArchive.getCanonicalName(), false, false);
			}
			else {
				validation.println("Renaming failed " + registerPath + "/" + archiveName);
				return false;
			}
		}

		// Rename new archive members
		for (final String file : listOfFiles) {
			if (commander.c_api.moveLFN(registerPath + "/" + file, parentdir + "/" + file) == null) {
				System.err.println("[" + new Date() + "] " + remoteFile + ": Failed to rename archive member " + parentdir + "/" + file);

				// Check if there is another copy of the same file in parentdir
				final LFN registerMember = commander.c_api.getLFN(registerPath + "/" + file);
				final LFN parentMember = commander.c_api.getLFN(parentdir + "/" + file);
				if (parentMember != null && parentMember.guid.equals(registerMember.guid)) {
					commander.c_api.removeLFN(registerMember.getCanonicalName(), false, false);
				}
				else {
					validation.println("Renaming failed " + parentdir + "/" + file);
					return false;
				}
			}
		}
		return true;
	}
}
