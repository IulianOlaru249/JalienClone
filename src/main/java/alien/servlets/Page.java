package alien.servlets;

import java.io.OutputStream;

import lazyj.ExtProperties;
import lazyj.page.BasePage;

/**
 * @author alina
 */
public class Page extends BasePage {
	private static ExtProperties globalProp = null;

	static {
		globalProp = new ExtProperties("config", "page");
	}

	@Override
	protected String getResDir() {
		return globalProp.gets("base.res.dir");
	}

	/**
	 * @param sFile
	 *            template file
	 */
	public Page(final String sFile) {
		this(null, sFile, false);
	}

	/**
	 * @param sFile
	 *            template file
	 * @param bCache
	 *            if the template is cached or not
	 */
	public Page(final String sFile, final boolean bCache) {
		this(null, sFile, bCache);
	}

	/**
	 * @param os
	 *            output stream
	 * @param sFile
	 *            template file
	 */
	public Page(final OutputStream os, final String sFile) {
		this(os, sFile, false);
	}

	/**
	 * @param os
	 *            output stream
	 * @param sFile
	 *            template file
	 * @param bCache
	 *            if the template should be cached or not
	 */
	public Page(final OutputStream os, final String sFile, final boolean bCache) {
		super(os, sFile, bCache);
	}
}
