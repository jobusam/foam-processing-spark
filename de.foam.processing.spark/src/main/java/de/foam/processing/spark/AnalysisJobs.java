package de.foam.processing.spark;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.input.PortableDataStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.foam.processing.spark.common.FileOutput;
import de.foam.processing.spark.common.FindDuplicates;
import de.foam.processing.spark.hashing.Hashing;
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
	 * Retrieve all available Metadata of HDFS files and write them into a text file
	 * 
	 * @param executionContext
	 */
	static void retrieveFileMetadata(JavaSparkContext executionContext, String dataDir, String outputDir) {
		try {
			FileMetadata.extractFileMetadata(executionContext, dataDir, outputDir);
		} catch (Exception e) {
			LOGGER.error("Retrieving Metadata of files in input directory {} failed!", dataDir, e);
		}

	}

	/**
	 * Find duplicate files dependent on their hashsum and persist the result as
	 * text file in readable format.
	 * 
	 * @param executionContext
	 */
	static void findDuplicateFiles(JavaSparkContext executionContext, String dataDir, String outputDir) {
		JavaPairRDD<String, PortableDataStream> filesWithBinContent = executionContext.binaryFiles(dataDir);
		try {
			JavaPairRDD<String, String> hashesWithFileNames = Hashing.hashFiles(filesWithBinContent);
			JavaPairRDD<String, String> results = FindDuplicates.filterForDuplicates(hashesWithFileNames);
			// printResults(results);
			FileOutput.saveResults(results, outputDir);
		} catch (Exception e) {
			LOGGER.error("Finding duplicated files on input {} failed!", dataDir, e);
		}
	}
}
