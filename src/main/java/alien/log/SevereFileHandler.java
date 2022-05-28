package alien.log;

import java.io.IOException;
import java.util.logging.FileHandler;
import java.util.logging.Filter;
import java.util.logging.Level;
import java.util.logging.LogRecord;

/**
 * @author Alina Grigoras sending SEVERE to a specific file
 *
 */
public class SevereFileHandler extends FileHandler {

	/**
	 * creating a simple FileHandler on which we apply SEVERE level + a filter to print only SEVER
	 * 
	 * @throws IOException
	 * @throws SecurityException
	 */
	public SevereFileHandler() throws IOException, SecurityException {
		super();
		// TODO Auto-generated constructor stub
	}

	@Override
	public synchronized void setLevel(final Level newLevel) throws SecurityException {
		// TODO Auto-generated method stub
		super.setLevel(Level.SEVERE);
	}

	@SuppressWarnings("sync-override")
	@Override
	public void setFilter(final Filter newFilter) throws SecurityException {
		// TODO Auto-generated method stub
		super.setFilter(new Filter() {

			@Override
			public boolean isLoggable(final LogRecord record) {
				// TODO Auto-generated method stub
				if (record.getLevel() != Level.SEVERE)
					return false;
				return true;
			}
		});
	}
}
