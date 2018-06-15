package de.foam.processing.spark;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.foam.processing.spark.common.FileOutput;
import de.foam.processing.spark.common.FindDuplicates;
import de.foam.processing.spark.hashing.Hashing;
import de.foam.processing.spark.hbase.HbaseConnector;
import de.foam.processing.spark.metadata.FileMetadata;

/**
 * Contains analysis jobs that could be executed on Spark
 * 
 * @author jobusam
 *
 */
public class AnalysisJobs {
	private static final Logger LOGGER = LoggerFactory.getLogger(AnalysisJobs.class);

	/**
	 * Calculate Hashsums of data files persisted in HBASE and HDFS and persists the
	 * result in HBASE!
	 * 
	 * @param executionContext
	 *            for loading data from HDFS
	 * @param hbc
	 *            {@link HbaseConnector} instance to connect to HBASE
	 * @param largeDataHdfsDir
	 *            is a hdfs file path to directory where large files are located
	 *            (see foam-data-import)
	 */
	static void calculateHashsums(JavaSparkContext executionContext, HbaseConnector hbc, String largeDataHdfsDir) {
		JavaPairRDD<String, byte[]> smallFileHashes = hbc.getSmallFileContent() // -
				.mapValues(Hashing::hashFiles);
		// .mapValues(Hashing::mapToHexString)
		// .take(5).forEach(h -> LOGGER.info("Small File {} hash = {}", h._1, h._2));
		hbc.putHashesToHbase(smallFileHashes);

		// Is it possible to load a hdfs file directly with the RDD on executor? It
		// looks like there is no method. So the binaryFiles() method is only available
		// on driver. But otherwise it does make sense, because loading an hdfs on any
		// predefined executor doesn't ensures that this file is really located on the
		// same cluster node (see data locality)
		JavaPairRDD<String, byte[]> largeFileHashes = executionContext.binaryFiles(largeDataHdfsDir) // -
				.mapValues(Hashing::hashFiles);
		// .mapValues(Hashing::mapToHexString)
		// .take(5).forEach(h -> LOGGER.info("Large File {} hash = {}", h._1, h._2));
		hbc.putHashesToHbase(largeFileHashes);
	}

	/**
	 * Find duplicate files dependent on their hashsum and persist the result as
	 * text file in readable format. FIXME: refactor and retrieve data from HBASE!
	 * 
	 * @param executionContext
	 */
	static void findDuplicateFiles(JavaSparkContext executionContext, String dataDir, String outputDir) {
		JavaPairRDD<String, String> hashesWithFileNames = executionContext.binaryFiles(dataDir)
				.mapValues(Hashing::hashFiles) // -
				.mapValues(Hashing::mapToHexString);
		try {
			JavaPairRDD<String, String> results = FindDuplicates.filterForDuplicates(hashesWithFileNames);
			// printResults(results);
			FileOutput.saveResults(results, outputDir);
		} catch (Exception e) {
			LOGGER.error("Finding duplicated files on input {} failed!", dataDir, e);
		}
	}

	/**
	 * Retrieve all available Metadata of HDFS files and write them into a text file
	 * 
	 * @param executionContext
	 * @deprecated the file metadata is already persisted in HBASE now. This method
	 *             tries to get the metadata only from hdfs files.
	 */
	@Deprecated
	static void retrieveFileMetadata(JavaSparkContext executionContext, String dataDir, String outputDir) {
		try {
			FileMetadata.extractFileMetadata(executionContext, dataDir, outputDir);
		} catch (Exception e) {
			LOGGER.error("Retrieving Metadata of files in input directory {} failed!", dataDir, e);
		}

	}
}
