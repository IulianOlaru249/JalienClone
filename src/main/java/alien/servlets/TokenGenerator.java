package alien.servlets;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import alien.taskQueue.JobToken;

/**
 * Generate unique job tokens
 *
 * @author costing
 * @since 2014-01-20
 */
public class TokenGenerator extends HttpServlet {

	/**
	 *
	 */
	private static final long serialVersionUID = 1599249953957590702L;

	@Override
	protected void doGet(final HttpServletRequest req, final HttpServletResponse resp) throws ServletException, IOException {
		try (ServletOutputStream os = resp.getOutputStream()) {
			os.println("$VAR1 = [");
			os.println("  {");
			os.println("    'token' => '" + JobToken.generateToken() + "'");
			os.println("  }");
			os.println("];");
		}
	}
}
