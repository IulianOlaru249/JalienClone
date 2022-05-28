package alien.log;

import java.io.IOException;
import java.util.logging.FileHandler;
import java.util.logging.Filter;
import java.util.logging.Level;
import java.util.logging.LogRecord;

/**
 * @author Alina Grigoras sending the FINER log to a specific file
 */
public class FinerFileHandler extends FileHandler {

	/**
	 * Creates a simple FileHandler On this handler we change the level and filter the output to the chosen level
	 * 
	 * @throws IOException
	 * @throws SecurityException
	 */
	public FinerFileHandler() throws IOException, SecurityException {
		super();
		// TODO Auto-generated constructor stub
	}

	@Override
	public synchronized void setLevel(final Level newLevel) throws SecurityException {
		// TODO Auto-generated method stub
		super.setLevel(Level.FINER);
	}

	@SuppressWarnings("sync-override")
	@Override
	public void setFilter(final Filter newFilter) throws SecurityException {
		// TODO Auto-generated method stub
		super.setFilter(new Filter() {

			@Override
			public boolean isLoggable(final LogRecord record) {
				// TODO Auto-generated method stub
				if (record.getLevel() != Level.FINER)
					return false;
				return true;
			}
		});
	}
}
