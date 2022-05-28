package alien.io.protocols;

import utils.StatusCode;
import utils.StatusType;

/**
 * @author Adrian Negru
 * @since Dec 14, 2020
 */
public enum SourceExceptionCode implements StatusCode {
    /**
     * File already exists
     */
    LOCAL_FILE_ALREADY_EXISTS("Server rejects the upload in case of an unforced attempt to upload an existing file", StatusType.INTERNAL_ERROR),

    /**
     * Xrdcp not in path
     */
    XRDCP_NOT_FOUND_IN_PATH("Xrootd binaries could not be located on the filesystem, can't work without them", StatusType.INTERNAL_ERROR),

    /**
     * Cannot confirm upload
     */
    XRDFS_CANNOT_CONFIRM_UPLOAD("Trying to get the file status with xrdfs failed to show the expected file details", StatusType.INTERNAL_ERROR),

    /**
     * Cannot start process
     */
    CANNOT_START_PROCESS("Java cannot launch other processes (out of handles / threads / other system resources?)", StatusType.INTERNAL_ERROR),

    /**
     * Interrupted while waiting command
     */
    INTERRUPTED_WHILE_WAITING_FOR_COMMAND("The Java process was interrupted (signal received from outside) while waiting for the transfer process to return", StatusType.INTERNAL_ERROR),

    /**
     * Xrootd timed out
     */
    XROOTD_TIMED_OUT("Transfer took longer than allowed (with the 20KB/s + 60s overhead) so xrdcp was forcefully terminated", StatusType.FILE_INACCESSIBLE),

    /**
     * Non-zero exit code for xrdcp
     */
    XROOTD_EXITED_WITH_CODE("Non-zero exit code from xrdcp, but no other details could be inferred from the command output", StatusType.INTERNAL_ERROR),

    /**
     * Downloaded local file size different
     */
    LOCAL_FILE_SIZE_DIFFERENT("Download seemed to have finished correctly, but the local file doesn't match the expected catalogue size", StatusType.FILE_CORRUPT),

    /**
     * Cannot create local file
     */
    LOCAL_FILE_CANNOT_BE_CREATED("For permissions or space reasons, the target local file could not be created", StatusType.INTERNAL_ERROR),

    /**
     * No servers have the file
     */
    NO_SERVERS_HAVE_THE_FILE("The server couldn't locate (by broadcasting) any replica of the requested file", StatusType.FILE_INACCESSIBLE),

    /**
     * No such file or directory
     */
    NO_SUCH_FILE_OR_DIRECTORY("The server returned an authoritative answer that the file is not present anywhere in its namespace", StatusType.FILE_INACCESSIBLE),

    /**
     * MD5 checksum differs
     */
    MD5_CHECKSUMS_DIFFER("The file could be read from the storage and the size matches, but the content has a different md5 checksum than the expected one", StatusType.FILE_CORRUPT),

    /**
     * The requested SE does not exist
     */
    SE_DOES_NOT_EXIST("The requested SE does not exist (any more)", StatusType.INTERNAL_ERROR),

    /**
     * The GET method of this protocol is not implemented
     */
    GET_METHOD_NOT_IMPLEMENTED("The GET method of this protocol is not implemented", StatusType.INTERNAL_ERROR),

    /**
     * Internal error
     */
    INTERNAL_ERROR("Any other error that doesn't fit anything from the above", StatusType.INTERNAL_ERROR);


    /**
     * Status description
     */
    private final String description;

    private final StatusType type;

    SourceExceptionCode(final String description, final StatusType type) {
        this.description = description;
        this.type = type;
    }

    /**
     * @return description of the status
     */
    @Override
	public String getDescription() {
        return description;
    }

    /**
     * @return type of the status
     */
    @Override
	public StatusType getType() {
        return type;
    }
}
