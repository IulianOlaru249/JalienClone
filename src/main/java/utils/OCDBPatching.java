package utils;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

import lazyj.DBFunctions;
import lazyj.Format;
import alien.config.ConfigUtils;

/**
 * @author costing
 * 
 */
public class OCDBPatching {

	private static final class OCDB {

		String file;

		int first_run;
		int last_run;
		int version;
		int dir_number;

		public OCDB(DBFunctions db) {
			file = db.gets("file");
			first_run = db.geti("first_run");
			last_run = db.geti("last_run");
			version = db.geti("version");
			dir_number = db.geti("dir_number");
		}

		@Override
		public String toString() {
			return file + " / " + first_run + " .. " + last_run + " (" + version + ") in " + dir_number;
		}

	}

	/**
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {
		int iCount = 0;

		try (DBFunctions db = ConfigUtils.getDB("alice_data"); PrintWriter pw = new PrintWriter(new FileWriter("ocdb.sql"))) {

			db.query("SELECT * FROM TaliprodVCDB WHERE first_run>0 and last_run=999999999 ORDER by dir_number ASC , version ASC;");

			// DBFunctions db2 = ConfigUtils.getDB("alice_data");

			OCDB prev = null;

			while (db.moveNext()) {
				OCDB ocdb = new OCDB(db);

				if (prev != null && prev.dir_number == ocdb.dir_number && ocdb.version > prev.version) {
					String q = "UPDATE TaliprodVCDB set last_run=" + (ocdb.first_run > prev.first_run ? ocdb.first_run - 1 : prev.first_run) + " WHERE file='" + Format.escSQL(prev.file) + "';";

					// db2.query(q);
					pw.println(q);

					iCount++;
				}

				prev = ocdb;
			}
		}

		System.err.println("Changed : " + iCount);
	}
}
