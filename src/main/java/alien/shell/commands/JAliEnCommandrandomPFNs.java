package alien.shell.commands;

import java.util.Collection;
import java.util.List;
import alien.catalogue.PFN;
import alien.se.SE;
import alien.se.SEUtils;
import alien.shell.ErrNo;
import lazyj.Format;

/**
 * @author anegru
 * @since 2020-03-02
 */
public class JAliEnCommandrandomPFNs extends JAliEnBaseCommand {

	private int seNumber;
	private int fileCount;

	@Override
	public void run() {
		final Collection<PFN> randomPFNs = commander.c_api.getRandomPFNsFromSE(seNumber, fileCount);

		if (randomPFNs != null) {
			for (final PFN p : randomPFNs)
				commander.printOutln(p.pfn + "\t" + Format.size(p.getGuid().size));
		}
	}

	@Override
	public void printHelp() {
		commander.printOutln();
		commander.printOutln("Extract <fileCount> random PFNs from the SE identified by its unique ID or its name");
		commander.printOutln(helpUsage("randomPFNs", "<seNumber or name> <fileCount>"));
		commander.printOutln();
	}

	@Override
	public boolean canRunWithoutArguments() {
		return false;
	}

	/**
	 * @param commander
	 * @param alArguments
	 */
	public JAliEnCommandrandomPFNs(final JAliEnCOMMander commander, final List<String> alArguments) {
		super(commander, alArguments);

		if (alArguments.size() != 2) {
			setArgumentsOk(false);
			return;
		}

		try {
			seNumber = Integer.parseInt(alArguments.get(0));
		}
		catch (@SuppressWarnings("unused") final NumberFormatException nfe) {
			// is this a SE name ?
			final SE se = SEUtils.getSE(alArguments.get(0));

			if (se != null)
				seNumber = se.seNumber;
			else {
				setArgumentsOk(false);
				commander.setReturnCode(ErrNo.ENXIO, "Unknown SE " + alArguments.get(0));
				return;
			}
		}

		try {
			fileCount = Integer.parseInt(alArguments.get(1));
		}
		catch (@SuppressWarnings("unused") final NumberFormatException nfe) {
			setArgumentsOk(false);
			commander.printErrln("Second argument should be the number of files to return. This is an unexpected value: " + alArguments.get(1));
			return;
		}

		setArgumentsOk(true);
	}
}
