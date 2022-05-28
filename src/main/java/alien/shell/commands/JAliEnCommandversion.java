package alien.shell.commands;

import java.util.Date;
import java.util.List;

import alien.config.Version;
import lazyj.Format;

/**
 * @author costing
 * @since 2020-09-15
 */
public class JAliEnCommandversion extends JAliEnBaseCommand {

	/**
	 * @param commander
	 * @param alArguments
	 */
	public JAliEnCommandversion(final JAliEnCOMMander commander, final List<String> alArguments) {
		super(commander, alArguments);
	}

	@Override
	public void run() {
		commander.printOutln("JAliEn tag: " + Version.getTag());
		commander.printOutln("Git hash: " + Version.getGitHash());
		commander.printOutln("Compiled at: " + Format.showDate(new Date(Version.getCompilationTimestamp())) + " @ " + Version.getCompilingHostname());

		commander.printOut("tag", Version.getTag());
		commander.printOut("git_hash", Version.getGitHash());
		commander.printOut("compiled_at", String.valueOf(Version.getCompilationTimestamp()));
		commander.printOut("compiled_by", Version.getCompilingHostname());
	}

	@Override
	public void printHelp() {
		commander.printOutln();
		commander.printOutln(helpUsage("version", ""));
		commander.printOutln();
		commander.printOutln(helpParameter("Get information about the JAliEn version that answers your requests"));
	}

	@Override
	public boolean canRunWithoutArguments() {
		return true;
	}
}
