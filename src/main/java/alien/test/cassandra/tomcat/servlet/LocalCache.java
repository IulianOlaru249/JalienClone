package alien.test.cassandra.tomcat.servlet;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import alien.catalogue.LFN_CSD;
import lazyj.RequestWrapper;

/**
 * Servlet to print local process cache statistics for Cassandra benchmark.
 *
 */
@WebServlet("/*")
public class LocalCache extends HttpServlet {
	private static final long serialVersionUID = 1L;
	private static final int DEFAULT_LIMIT = 500;

	@Override
	protected void doGet(final HttpServletRequest request, final HttpServletResponse response) throws ServletException, IOException {
		response.setContentType("text/plain");
		@SuppressWarnings("resource")
		PrintWriter pw = response.getWriter();
		RequestWrapper rw = new RequestWrapper(request);

		int limit = rw.geti("limit", DEFAULT_LIMIT);
		String path = rw.gets("path", null);

		ArrayList<String> keys_array = new ArrayList<>(LFN_CSD.dirCache.getKeys());

		if (path != null) {
			if (keys_array.contains(path)) {
				pw.println(LFN_CSD.dirCache.get(path));
			}
			else {
				pw.println("Entry not found: " + path);
			}
		}
		else {
			int counter = 0;
			for (int i = keys_array.size() - 1; i >= 0; i--) {
				pw.println(keys_array.get(i));
				counter++;
				if (counter > limit)
					break;
			}
		}
		pw.flush();
	}

}
