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
 * Following functionality is implemented:<br>
 * - Calculate file hashes and persist it <br>
 * - Detect Media-Type of files and persist it<br>
 * - Determine duplicate files (output in log file)<br>
 * --- TODO: How to save the results?<br>
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

	// FIXME: Make directory configurable!
	private String LARGE_FILES_HDFS_PATH = "/data/";
	Optional<Path> hbaseConfigFile = Optional.empty();

	public static void main(String[] args) {
		new ForensicAnalysis().analyzeData(args);
	}

	void analyzeData(String[] args) {
		validateInputParams(args);
		SparkConf sparkConf = new SparkConf().setAppName("ForensicAnalysis");
		JavaSparkContext jsc = new JavaSparkContext(sparkConf);
		try {
			HbaseConnector hbc = new HbaseConnector(jsc, hbaseConfigFile);

			AnalysisJobs.calculateHashsums(jsc, hbc, LARGE_FILES_HDFS_PATH);
			// AnalysisJobs.findDuplicateFiles(jsc, hbc); // requires file hashes in hbase

			AnalysisJobs.detectFileMediaType(jsc, hbc, LARGE_FILES_HDFS_PATH);
			// AnalysisJobs.printMediaTypesAndAmount(hbc);// requires media types in hbase

			// Only for testing purpose!
			// AnalysisJobs.indexHbaseContentWithSolr(jsc, hbc);
			// AnalysisJobs.printDifferentMediaTypes(jsc, hbc, LARGE_FILES_HDFS_PATH);
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
	 * hbase-site.xml, because this configuration defines how to access HBASE!<br>
	 * The second parameter is the hdfs file path, where large files are persisted.
	 * <br>
	 */
	private void validateInputParams(String[] args) {
		if (args.length >= 1 && args[0] != null && Paths.get(args[0]) != null) {
			Path path = Paths.get(args[0]);
			if (path != null && path.toFile().exists()) {
				LOGGER.info("Use file {} as hbase-site.xml file to connect to HBASE", path);
				hbaseConfigFile = Optional.of(path);
			} else {
				LOGGER.error("The given parameter {} is no valid file path to hbase-site.xml configuration file",
						args[0]);
			}
		} else {
			LOGGER.info("Usage example: ForensicAnalysis [path_to_hbase-site.xml] [hdfs_path_to_large_data_files]\n"
					+ "If no parameter is given default values will be set to access hbase.");
		}

		if (args.length >= 2 && args[1] != null && Paths.get(args[1]) != null) {
			LARGE_FILES_HDFS_PATH = args[1];
		} else {
			LOGGER.error("The given parameter {} is no valid file path to hbase-site.xml configuration file", args[1]);
		}
		LOGGER.info("HDFS Large File Directory is {}", args[1]);
	}

}