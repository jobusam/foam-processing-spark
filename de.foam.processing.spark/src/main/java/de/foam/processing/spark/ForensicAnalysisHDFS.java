package de.foam.processing.spark;

import java.nio.file.Paths;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@link ForensicAnalysisHDFS} Application creates the environment for
 * executing forensic analysis jobs.
 * 
 * @author jobusam
 * @deprecated use {@link ForensicAnalysis} instead! This version is only
 *             available to test raw hdfs throughput and compare results with
 *             new HDFS-HBASE persistence approach.
 */
@Deprecated
public class ForensicAnalysisHDFS {

	private static String dataDir;
	private static String outputDir;
	private static final Logger LOGGER = LoggerFactory.getLogger(ForensicAnalysisHDFS.class);

	public static void main(String[] args) {

		boolean validParams = validateInputParams(args);
		if (!validParams) {
			System.exit(1);
		}

		JavaSparkContext executionContext = createExecutionContext();

		// Execute jobs
		// AnalysisJobs.retrieveFileMetadata(executionContext, dataDir, outputDir +
		// "/metadata");
		AnalysisJobs.findDuplicateFiles(executionContext, dataDir, outputDir + "/duplicatefiles");
		executionContext.stop();
	}

	/**
	 * Check and set input parameters. For every execution an Input and Output
	 * directory must be given!
	 * 
	 * @param args
	 * @return true if the parameters are valid
	 */
	private static boolean validateInputParams(String[] args) {
		if (args.length == 2) {
			// Normally for input directory it's possible to check if path is a directory.
			// But this doesn't work in case the directory is located in HDFS
			if (args[0] != null && Paths.get(args[0]) != null) {
				if (args[1] != null && Paths.get(args[1]) != null) {
					dataDir = args[0];
					outputDir = args[1];
					LOGGER.info("InputDirectory is {}", dataDir);
					LOGGER.info("OuputDirectory for Results is {}", outputDir);
					return true;
				} else {
					LOGGER.error(
							"<OutputDirectory> is invalid. Stop execution. Usage example: ForensicAnalysis <InputDirectory> <OuputDirectory>");
				}
			} else {
				LOGGER.error(
						"<InputDirectory> is invalid. Stop execution. Usage example: ForensicAnalysis <InputDirectory> <OuputDirectory>");
			}
		} else {
			LOGGER.error(
					"InputDirectory and OutputDirectory are missing!!!! Usage example: ForensicAnalysis <InputDirectory> <OuputDirectory>");
		}
		return false;
	}

	public static JavaSparkContext createExecutionContext() {
		// Create a Spark Context
		SparkConf conf = new SparkConf();
		JavaSparkContext jsc = new JavaSparkContext(conf);
		// is important for retrieving all Files in a directory recursively (see method
		// JavaSparkContext#wholeTextFiles)
		jsc.hadoopConfiguration().set("mapreduce.input.fileinputformat.input.dir.recursive", "true");
		return jsc;
	}

}
