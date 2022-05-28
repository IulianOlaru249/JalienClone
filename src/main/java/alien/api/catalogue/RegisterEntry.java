package alien.api.catalogue;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import alien.api.Request;
import alien.catalogue.Register;
import alien.config.ConfigUtils;
import alien.site.OutputEntry;
import alien.user.AliEnPrincipal;

/**
 * @author vyurchen
 *
 */
public class RegisterEntry extends Request {
	/**
	 * Logger
	 */
	static transient final Logger logger = ConfigUtils.getLogger(RegisterEntry.class.getCanonicalName());

	private static final long serialVersionUID = -2004904530203513524L;
	private final OutputEntry entry;
	private final String outputDir;
	private boolean wasRegistered;

	/**
	 * @param entry lfn entry to register (converted to OutputEntry)
	 * @param outputDir absolute path
	 * @param user
	 */
	public RegisterEntry(final OutputEntry entry, final String outputDir, final AliEnPrincipal user) {
		setRequestUser(user);
		this.entry = entry;
		this.outputDir = outputDir;
		this.wasRegistered = false;
	}

	@Override
	public List<String> getArguments() {
		return Arrays.asList(this.outputDir, this.entry != null ? this.entry.toString() : null);
	}

	@Override
	public void run() {
		if (entry != null && outputDir != null && outputDir.length() != 0) {
			try {
				wasRegistered = Register.register(entry, outputDir, getEffectiveRequester());
			}
			catch (final IOException e) {
				logger.log(Level.SEVERE, "Could not register entry " + entry.getName());
				e.printStackTrace();
			}
		}
		else {
			logger.log(Level.SEVERE, "Invalid arguments in RegisterLFN");
		}
	}

	/**
	 * @return the status of the registration of the LFN(s)
	 */
	public boolean wasRegistered() {
		return this.wasRegistered;
	}
}
