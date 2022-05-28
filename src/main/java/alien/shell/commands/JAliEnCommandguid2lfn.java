package alien.shell.commands;

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.UUID;

import alien.catalogue.GUID;
import alien.catalogue.GUIDUtils;
import alien.catalogue.LFN;
import alien.shell.ErrNo;
import joptsimple.OptionException;

/**
 * @author ron
 * @since June 4, 2011
 */
public class JAliEnCommandguid2lfn extends JAliEnBaseCommand {

	/**
	 * entry the call is executed on, either representing a LFN
	 */
	private final Collection<String> guidNames = new LinkedHashSet<>();

	/**
	 * execute the lfn2guid
	 */
	@Override
	public void run() {
		final Set<UUID> uuids = new LinkedHashSet<>();
		for (final String guidName : guidNames) {
			try {
				uuids.add(UUID.fromString(guidName));
			}
			catch (@SuppressWarnings("unused") final Throwable t) {
				commander.setReturnCode(ErrNo.EINVAL, "Not a GUID: " + guidName);
				return;
			}
		}

		final Collection<GUID> guids = commander.c_api.getGUIDs(uuids, true);

		for (final GUID guid : guids) {
			uuids.remove(guid.guid);

			final Iterator<LFN> it;

			commander.printOut("guid", String.valueOf(guid.guid));

			if (guid.getLFNs() != null && (it = guid.getLFNs().iterator()).hasNext()) {
				final LFN lfn = it.next();

				commander.printOutln(padRight(guid.guid + "", 40) + lfn.getCanonicalName());

				commander.printOut("lfn", String.valueOf(lfn.getCanonicalName()));
			}
			else {
				commander.printOut("error", "No LFN");
				commander.setReturnCode(ErrNo.ENOENT, "No LFNs are associated to this GUID [" + guid.guid + "].");
			}

			commander.outNextResult();
		}

		// what's left here are UUIDs for which no GUID object could be found in the database
		for (final UUID uuid : uuids) {
			commander.printOut("guid", uuid.toString());
			commander.printOut("error", "No GUID");

			commander.setReturnCode(ErrNo.ENOENT, "GUID cannot be found in the database: " + uuid);

			commander.outNextResult();
		}
	}

	/**
	 * printout the help info
	 */
	@Override
	public void printHelp() {
		commander.printOutln();
		commander.printOutln(helpUsage("guid2lfn", "<GUID>"));
		commander.printOutln();
	}

	/**
	 * guid2lfn cannot run without arguments
	 *
	 * @return <code>false</code>
	 */
	@Override
	public boolean canRunWithoutArguments() {
		return false;
	}

	/**
	 * Constructor needed for the command factory in JAliEnCOMMander
	 *
	 * @param commander
	 *
	 * @param alArguments
	 *            the arguments of the command
	 * @throws OptionException
	 */
	public JAliEnCommandguid2lfn(final JAliEnCOMMander commander, final List<String> alArguments) throws OptionException {
		super(commander, alArguments);

		if (alArguments.size() == 0) {
			// help will be printed by the commander anyway since canRunWithoutArguments=false
			return;
		}

		for (final String s : alArguments) {
			boolean ok = false;

			final StringTokenizer st = new StringTokenizer(s, " \r\n\t;#/\\?");

			while (st.hasMoreTokens()) {
				final String tok = st.nextToken();

				if (GUIDUtils.isValidGUID(tok)) {
					guidNames.add(tok);
					ok = true;
					continue;
				}
			}

			if (!ok) {
				commander.setReturnCode(ErrNo.EINVAL, "No GUID in this string: " + s);
				setArgumentsOk(false);
				return;
			}
		}
	}
}
