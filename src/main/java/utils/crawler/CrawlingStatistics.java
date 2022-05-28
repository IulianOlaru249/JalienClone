package utils.crawler;

import java.util.List;

import org.json.simple.JSONObject;

/**
 * Class that models the crawling statistics that are gathered in each iteration.
 * All fields are nullable, thus optional. If one field is null it means that it cannot be retrieved
 * from disk or other errors occurred. Null values are skipped.
 *
 * @author anegru
 */
public class CrawlingStatistics {

	/**
	 * The total number of PFNs analysed.
	 */
	public long pfnCount;

	/**
	 * The number of PFNs that are ok from the total number of PFNs crawled
	 */
	public long pfnOkCount;

	/**
	 * The number of PFNs that are inaccessible from the total number of PFNs crawled
	 */
	public long pfnInaccessibleCount;

	/**
	 * The number of PFNs that are corrupt from the total number of PFNs crawled
	 */
	public long pfnCorruptCount;

	/**
	 * The number of PFNs whose status is unknown from the total number of PFNs crawled
	 */
	public long pfnUnknownStatusCount;

	/**
	 * Average duration in milliseconds of the crawling process
	 */
	public long crawlingAvgDurationMillis;

	/**
	 * Total duration in milliseconds of the crawling process
	 */
	public long crawlingTotalDurationMillis;

	/**
	 * Total size of files in bytes downloaded during the crawling process
	 */
	public long fileSizeTotalBytes;

	/**
	 * Total duration of file download in milliseconds
	 */
	public long downloadTotalDurationMillis;

	/**
	 * Total number of PFNs that could be downloaded
	 */
	public long downloadedPFNsTotalCount;

	/**
	 * Total duration of xrdfs in milliseconds
	 */
	public long xrdfsTotalDurationMillis;

	/**
	 * Total number of PFNs that were tested with xrdfs
	 */
	public long xrdfsPFNsTotalCount;

	/**
	 * The Unix timestamp when these statistics are written to disk
	 */
	public long statGeneratedUnixTimestamp;

	private static final int NULL_VALUE = -1;
	private static final int DEFAULT_VALUE = 0;

	private CrawlingStatistics() {
		this.pfnCount = NULL_VALUE;
		this.pfnOkCount = NULL_VALUE;
		this.pfnInaccessibleCount = NULL_VALUE;
		this.pfnCorruptCount = NULL_VALUE;
		this.pfnUnknownStatusCount = NULL_VALUE;
		this.crawlingAvgDurationMillis = NULL_VALUE;
		this.crawlingTotalDurationMillis = NULL_VALUE;
		this.fileSizeTotalBytes = NULL_VALUE;
		this.downloadTotalDurationMillis = NULL_VALUE;
		this.downloadedPFNsTotalCount = NULL_VALUE;
		this.xrdfsTotalDurationMillis = NULL_VALUE;
		this.xrdfsPFNsTotalCount = NULL_VALUE;
		this.statGeneratedUnixTimestamp = NULL_VALUE;
	}

	/**
	 * @param pfnCount
	 * @param pfnOkCount
	 * @param pfnInaccessibleCount
	 * @param pfnCorruptCount
	 * @param pfnUnknownStatusCount
	 * @param crawlingAvgDurationMillis
	 * @param crawlingTotalDurationMillis
	 * @param fileSizeTotalBytes
	 * @param downloadedPFNsTotalCount
	 * @param downloadTotalDurationMillis
	 * @param xrdfsPFNsTotalCount
	 * @param xrdfsTotalDurationMillis
	 * @param statGeneratedUnixTimestamp
	 */
	public CrawlingStatistics(final long pfnCount, final long pfnOkCount, final long pfnInaccessibleCount, final long pfnCorruptCount, final long pfnUnknownStatusCount,
			final long crawlingAvgDurationMillis, final long crawlingTotalDurationMillis, final long fileSizeTotalBytes,
			final long downloadedPFNsTotalCount, final long downloadTotalDurationMillis, final long xrdfsPFNsTotalCount, final long xrdfsTotalDurationMillis, final long statGeneratedUnixTimestamp) {
		this.pfnCount = pfnCount;
		this.pfnOkCount = pfnOkCount;
		this.pfnInaccessibleCount = pfnInaccessibleCount;
		this.pfnCorruptCount = pfnCorruptCount;
		this.pfnUnknownStatusCount = pfnUnknownStatusCount;
		this.crawlingAvgDurationMillis = crawlingAvgDurationMillis;
		this.crawlingTotalDurationMillis = crawlingTotalDurationMillis;
		this.fileSizeTotalBytes = fileSizeTotalBytes;
		this.downloadedPFNsTotalCount = downloadedPFNsTotalCount;
		this.downloadTotalDurationMillis = downloadTotalDurationMillis;
		this.xrdfsPFNsTotalCount = xrdfsPFNsTotalCount;
		this.xrdfsTotalDurationMillis = xrdfsTotalDurationMillis;
		this.statGeneratedUnixTimestamp = statGeneratedUnixTimestamp;
	}

	/**
	 * @param jsonObject
	 * @return CrawlingStatistics from JSONObject
	 */
	public static CrawlingStatistics fromJSON(final JSONObject jsonObject) {
		return new CrawlingStatistics(
				getLongFromJSON(jsonObject, "pfnCount"),
				getLongFromJSON(jsonObject, "pfnOkCount"),
				getLongFromJSON(jsonObject, "pfnInaccessibleCount"),
				getLongFromJSON(jsonObject, "pfnCorruptCount"),
				getLongFromJSON(jsonObject, "pfnUnknownStatusCount"),
				getLongFromJSON(jsonObject, "crawlingAvgDurationMillis"),
				getLongFromJSON(jsonObject, "crawlingTotalDurationMillis"),
				getLongFromJSON(jsonObject, "fileSizeTotalBytes"),
				getLongFromJSON(jsonObject, "downloadedPFNsTotalCount"),
				getLongFromJSON(jsonObject, "downloadTotalDurationMillis"),
				getLongFromJSON(jsonObject, "xrdfsPFNsTotalCount"),
				getLongFromJSON(jsonObject, "xrdfsTotalDurationMillis"),
				getLongFromJSON(jsonObject, "statGeneratedUnixTimestamp"));
	}

	/**
	 * @param stats
	 * @return JSONObject from CrawlingStatistics
	 */
	@SuppressWarnings("unchecked")
	public static JSONObject toJSON(final CrawlingStatistics stats) {

		final JSONObject json = new JSONObject();

		json.put("pfnCount", getValueForJSON(stats.pfnCount));
		json.put("pfnOkCount", getValueForJSON(stats.pfnOkCount));
		json.put("pfnInaccessibleCount", getValueForJSON(stats.pfnInaccessibleCount));
		json.put("pfnCorruptCount", getValueForJSON(stats.pfnCorruptCount));
		json.put("pfnUnknownStatusCount", getValueForJSON(stats.pfnUnknownStatusCount));
		json.put("crawlingAvgDurationMillis", getValueForJSON(stats.crawlingAvgDurationMillis));
		json.put("crawlingTotalDurationMillis", getValueForJSON(stats.crawlingTotalDurationMillis));
		json.put("fileSizeTotalBytes", getValueForJSON(stats.fileSizeTotalBytes));
		json.put("downloadedPFNsTotalCount", getValueForJSON(stats.downloadedPFNsTotalCount));
		json.put("downloadTotalDurationMillis", getValueForJSON(stats.downloadTotalDurationMillis));
		json.put("xrdfsPFNsTotalCount", getValueForJSON(stats.xrdfsPFNsTotalCount));
		json.put("xrdfsTotalDurationMillis", getValueForJSON(stats.xrdfsTotalDurationMillis));
		json.put("statGeneratedUnixTimestamp", getValueForJSON(stats.statGeneratedUnixTimestamp));

		return json;
	}

	private static Long getValueForJSON(final long value) {
		return value != NULL_VALUE ? Long.valueOf(value) : null;
	}

	/**
	 * @param statsList
	 * @return the average statistics from a list of CrawlingStatistics objects
	 */
	public static CrawlingStatistics getAveragedStats(final List<CrawlingStatistics> statsList) {

		if (statsList == null || statsList.size() == 0)
			return null;

		final CrawlingStatistics averagedStats = new CrawlingStatistics();
		int crawlingAvgDurationCount = 0;

		for (final CrawlingStatistics stats : statsList) {

			if (stats.pfnOkCount != NULL_VALUE) {
				averagedStats.pfnOkCount = initializeIfNull(averagedStats.pfnOkCount);
				averagedStats.pfnOkCount += stats.pfnOkCount;
			}

			if (stats.pfnInaccessibleCount != NULL_VALUE) {
				averagedStats.pfnInaccessibleCount = initializeIfNull(averagedStats.pfnInaccessibleCount);
				averagedStats.pfnInaccessibleCount += stats.pfnInaccessibleCount;
			}

			if (stats.pfnCorruptCount != NULL_VALUE) {
				averagedStats.pfnCorruptCount = initializeIfNull(averagedStats.pfnCorruptCount);
				averagedStats.pfnCorruptCount += stats.pfnCorruptCount;
			}

			if (stats.pfnUnknownStatusCount != NULL_VALUE) {
				averagedStats.pfnUnknownStatusCount = initializeIfNull(averagedStats.pfnUnknownStatusCount);
				averagedStats.pfnUnknownStatusCount += stats.pfnUnknownStatusCount;
			}

			if (stats.fileSizeTotalBytes != NULL_VALUE) {
				averagedStats.fileSizeTotalBytes = initializeIfNull(averagedStats.fileSizeTotalBytes);
				averagedStats.fileSizeTotalBytes += stats.fileSizeTotalBytes;
			}

			if (stats.crawlingAvgDurationMillis != NULL_VALUE) {
				averagedStats.crawlingAvgDurationMillis = initializeIfNull(averagedStats.crawlingAvgDurationMillis);
				averagedStats.crawlingAvgDurationMillis += stats.crawlingAvgDurationMillis;
				crawlingAvgDurationCount += 1;
			}

			if (stats.pfnCount != NULL_VALUE) {
				averagedStats.pfnCount = initializeIfNull(averagedStats.pfnCount);
				averagedStats.pfnCount += stats.pfnCount;
			}

			if (stats.crawlingTotalDurationMillis != NULL_VALUE) {
				averagedStats.crawlingTotalDurationMillis = initializeIfNull(averagedStats.crawlingTotalDurationMillis);
				averagedStats.crawlingTotalDurationMillis += stats.crawlingTotalDurationMillis;
			}

			if (stats.downloadedPFNsTotalCount != NULL_VALUE) {
				averagedStats.downloadedPFNsTotalCount = initializeIfNull(averagedStats.downloadedPFNsTotalCount);
				averagedStats.downloadedPFNsTotalCount += stats.downloadedPFNsTotalCount;
			}

			if (stats.downloadTotalDurationMillis != NULL_VALUE) {
				averagedStats.downloadTotalDurationMillis = initializeIfNull(averagedStats.downloadTotalDurationMillis);
				averagedStats.downloadTotalDurationMillis += stats.downloadTotalDurationMillis;
			}

			if (stats.xrdfsPFNsTotalCount != NULL_VALUE) {
				averagedStats.xrdfsPFNsTotalCount = initializeIfNull(averagedStats.xrdfsPFNsTotalCount);
				averagedStats.xrdfsPFNsTotalCount += stats.xrdfsPFNsTotalCount;
			}

			if (stats.xrdfsTotalDurationMillis != NULL_VALUE) {
				averagedStats.xrdfsTotalDurationMillis = initializeIfNull(averagedStats.xrdfsTotalDurationMillis);
				averagedStats.xrdfsTotalDurationMillis += stats.xrdfsTotalDurationMillis;
			}
		}

		averagedStats.crawlingAvgDurationMillis = crawlingAvgDurationCount == 0 ? NULL_VALUE : averagedStats.crawlingAvgDurationMillis / crawlingAvgDurationCount;
		averagedStats.statGeneratedUnixTimestamp = System.currentTimeMillis();

		return averagedStats;
	}

	private static long initializeIfNull(final long value) {
		return value == NULL_VALUE ? DEFAULT_VALUE : value;
	}

	private static long getLongFromJSON(final JSONObject json, final String key) {
		try {
			return ((Long) json.get(key)).longValue();
		}
		catch (final Exception e) {
			e.printStackTrace();
			return NULL_VALUE;
		}
	}

	@Override
	public String toString() {
		return toJSON(this).toJSONString();
	}
}
