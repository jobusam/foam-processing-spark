package de.foam.processing.spark;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.foam.processing.spark.hbase.Content;
import de.foam.processing.spark.hbase.HbaseConnector;

/**
 * The {@link ForensicAnalysis} class reads data from HBASE and prints some
 * results.<br>
 * TODO: Implement calculation of file content hashes (almost done!)<br>
 * TODO: Determine duplicate files<br>
 * TODO: Analyse File Mime-Type of files<br>
 * TODO: Extract strings from file content for full text search / indexing<br>
 * <br>
 * The data model in HBASE is derived from foam-data-import project.<br>
 * 
 * @author jobusam
 * 
 * @see <a href=
 *      "https://github.com/jobusam/foam-data-import">foam-data-import</a>
 */
final public class ForensicAnalysis {
	private static final Logger LOGGER = LoggerFactory.getLogger(ForensicAnalysis.class);

	public static void main(String[] args) {
		Optional<Path> hbaseConfigFile = validateInputParams(args);
		SparkConf sparkConf = new SparkConf().setAppName("ForensicAnalysis");
		JavaSparkContext jsc = new JavaSparkContext(sparkConf);
		try {
			HbaseConnector hbc = new HbaseConnector(jsc, hbaseConfigFile);
			// FIXME: Make directory configurable!
			AnalysisJobs.calculateHashsums(jsc, hbc, "/data/");
			AnalysisJobs.findDuplicateFiles(jsc, hbc);
		} finally {
			jsc.stop();
		}
	}

	/**
	 * Additional testing calls -> NON-Productive!
	 */
	private void testCalls(HbaseConnector hbc) {

		LOGGER.info("Small files in HBASE = {}", hbc.getSmallFileContent().count());
		hbc.getSmallFileContent().mapValues(bb -> bb.capacity()).take(10)
				.forEach(p -> LOGGER.info("Row {} : fileSize = {}", p._1, p._2));

		long count = hbc.getForensicMetadata().count();
		LOGGER.info("Count = {}", count);

		hbc.getForensicMetadata().take(10).stream()
				.forEach(e -> LOGGER.info("Entry = {} and relativePath = {} and file size = {}.", e.getId(),
						e.getRelativeFilePath(), e.getFileSize()));

		hbc.getForensicFileContent() // -
				.filter(c -> c.getHdfsFilePath() != null && !c.getHdfsFilePath().isEmpty())
				.map((Content e) -> String.format(
						"Entry = %s with relativePath = %s and hdfsPath = %s and hbase content size =%d.", e.getId(),
						e.getRelativeFilePath(), e.getHdfsFilePath(),
						e.getContent() != null ? e.getContent().capacity() : -1))
				// Keep in mind the data type Content is not serializable due to the containing
				// ByteBuffer. Thats the reason why the content's data will converted in single
				// string message BEFORE the collect() method is called. Because after the
				// collect everything must be available on the driver!
				.take(20).stream().forEach(e -> LOGGER.info(e));
	}

	/**
	 * Check if input parameters are set. It's possible to set the path to
	 * hbase-site.xml, because this configuration defines how to access HBASE!
	 * 
	 * @param args
	 */
	private static Optional<Path> validateInputParams(String[] args) {
		if (args.length >= 1 && args[0] != null && Paths.get(args[0]) != null) {
			Path path = Paths.get(args[0]);
			if (path != null && path.toFile().exists()) {
				LOGGER.info("Use file {} as hbase-site.xml file to connect to HBASE", path);
				return Optional.of(path);
			}
			LOGGER.error("The given parameter {} is no valid file path to hbase-site.xml configuration file", args[0]);
		}
		LOGGER.info(
				"No parameter set for hbase-site.xml path. Use default configuration for localhost to connect to HBASE.\n"
						+ "Usage example: ForensicAnalysis <path_to_hbase-site.xml> (Optional)");
		return Optional.empty();
	}

}