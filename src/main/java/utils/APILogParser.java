package utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import alien.catalogue.LFN;
import alien.catalogue.LFNUtils;
import alien.catalogue.PFN;
import alien.se.SE;
import alien.se.SEUtils;
import lazyj.Format;
import lazyj.cache.GenericLastValuesCache;

/**
 * Utility class for parsing API Server logs and produce the file access tuples needed by the Quantum project
 *
 * @author costing
 * @since 2019-12-16
 */
public class APILogParser {

	@SuppressWarnings("serial")
	private static GenericLastValuesCache<String, LFN> lfnCache = new GenericLastValuesCache<>() {
		@Override
		protected LFN resolve(final String key) {
			return LFNUtils.getLFN(key);
		}

		@Override
		protected boolean cacheNulls() {
			return true;
		}

		@Override
		protected int getMaximumSize() {
			return 200000;
		}
	};

	@SuppressWarnings("serial")
	private static GenericLastValuesCache<LFN, Set<PFN>> whereisCache = new GenericLastValuesCache<>() {
		@Override
		protected Set<PFN> resolve(final LFN key) {
			return key.whereisReal();
		}

		@Override
		protected boolean cacheNulls() {
			return true;
		}

		@Override
		protected int getMaximumSize() {
			return 200000;
		}
	};

	/**
	 * Parse all api server logs received on stdin
	 *
	 * @param args
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public static void main(final String[] args) throws IOException, InterruptedException {
		final String searchFor = " access read ";

		final String jsonSearch = "\"command\":\"access\",\"arguments\":[\"read\"";

		final CachedThreadPool mtResolver = new CachedThreadPool(12, 5, TimeUnit.SECONDS);

		final long startTime = args.length > 0 ? Long.parseLong(args[0]) : 0;
		final long endTime = args.length > 1 ? Long.parseLong(args[1]) : Long.MAX_VALUE;

		try (BufferedReader br = new BufferedReader(new InputStreamReader(System.in)); PrintWriter pw = new PrintWriter("apicommands.log")) {
			String line;

			int lineNo = 0;
			int accessNo = 0;

			pw.println("#epoch time,file name,size,accessed from,sorted replica list");

			while ((line = br.readLine()) != null) {
				if (++lineNo % 10000 == 0) {
					System.err.print("Reached line " + lineNo + ", out of which " + accessNo + " were accesses, queue len is " + mtResolver.getQueue().size());

					while (mtResolver.getQueue().size() > 10000) {
						System.err.print(".");

						Thread.sleep(1000);
					}

					System.err.println();
				}

				int idx = line.indexOf(searchFor);

				final long timestamp;

				final String lfn;

				final String site;

				if (idx >= 0) {
					final String date = line.substring(8, 37).trim();

					final Date d = Format.parseDate(date);

					timestamp = d.getTime();

					idx += searchFor.length();

					lfn = line.substring(idx, line.indexOf(' ', idx + 1));

					site = line.substring(line.lastIndexOf(' ') + 1);
				}
				else if (line.indexOf(jsonSearch) > 0) {
					final JSONParser parser = new JSONParser();

					try {
						final JSONObject entry = (JSONObject) parser.parse(line);

						timestamp = ((Long) entry.get("timestamp")).longValue();

						site = entry.get("site").toString();
						lfn = ((JSONArray) entry.get("arguments")).get(1).toString();
					}
					catch (final Exception e) {
						System.err.println("Exception json parsing " + line + ": " + e.getMessage());
						continue;
					}
				}
				else
					continue;

				if (timestamp < startTime && timestamp > endTime)
					continue;

				accessNo++;

				mtResolver.submit(() -> {
					final LFN l = lfnCache.get(lfn);

					if (l == null)
						return;

					final Collection<PFN> replicas = whereisCache.get(l);

					if (replicas == null || replicas.size() == 0)
						return;

					String replicaOrder;

					if (replicas.size() > 1) {
						final List<PFN> sortedSites = SEUtils.sortBySite(replicas, site, false, false);

						replicaOrder = sortedSites.stream().map(PFN::getSE).map(SE::getName).collect(Collectors.joining(";"));
					}
					else {
						replicaOrder = replicas.iterator().next().getSE().getName();
					}

					final String outString = timestamp + "," + l.getCanonicalName() + "," + l.getSize() + "," + site + "," + replicaOrder;

					synchronized (pw) {
						pw.println(outString);
					}
				});
			}

			while (mtResolver.getActiveCount() > 0 && mtResolver.getQueue().size() > 0) {
				System.err.println((new Date()) + " : waiting for " + mtResolver.getActiveCount() + " threads to process the remaining " + mtResolver.getQueue().size() + " files");

				Thread.sleep(10000);
			}
		}
	}
}
