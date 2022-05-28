package alien.shell.commands;

import java.util.List;

import alien.catalogue.Package;
import alien.shell.ErrNo;

/**
 * @author ron
 * @since Nov 23, 2011
 */
public class JAliEnCommandpackages extends JAliEnBaseCommand {

	private List<Package> packs = null;

	@Override
	public void run() {

		packs = commander.c_api.getPackages(getPackagePlatformName());

		if (packs != null) {
			for (final Package p : packs) {
				commander.outNextResult();
				commander.printOut("packages", p.getFullName());
				commander.printOutln(padSpace(1) + p.getFullName());
			}
		}
		else {
			commander.setReturnCode(ErrNo.ENODATA, "Couldn't find any packages.");
		}
	}

	private static String getPackagePlatformName() {

		String ret = System.getProperty("os.name");

		if (System.getProperty("os.arch").contains("amd64"))
			ret += "-x86_64";

		else if (ret.toLowerCase().contains("mac") && System.getProperty("os.arch").contains("ppc"))
			ret = "Darwin-PowerMacintosh";

		return ret;
	}

	/**
	 * printout the help info, none for this command
	 */
	@Override
	public void printHelp() {
		commander.printOutln();
		commander.printOutln(helpUsage("packages", "  list available packages"));
		commander.printOutln();
	}

	/**
	 * cd can run without arguments
	 *
	 * @return <code>true</code>
	 */
	@Override
	public boolean canRunWithoutArguments() {
		return true;
	}

	/**
	 * Constructor needed for the command factory in commander
	 *
	 * @param commander
	 *
	 * @param alArguments
	 *            the arguments of the command
	 */
	public JAliEnCommandpackages(final JAliEnCOMMander commander, final List<String> alArguments) {
		super(commander, alArguments);
	}
}
