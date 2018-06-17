package de.foam.processing.spark;

import java.nio.ByteBuffer;
import java.nio.file.Paths;

import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.input.PortableDataStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.foam.processing.spark.common.FindDuplicates;
import de.foam.processing.spark.common.MediaTypeDetection;
import de.foam.processing.spark.hashing.Hashing;
import de.foam.processing.spark.hbase.HbaseConnector;
import scala.Tuple2;

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
	 * @param jsc
	 *            for loading data from HDFS
	 * @param hbc
	 *            {@link HbaseConnector} instance to connect to HBASE
	 * @param largeDataHdfsDir
	 *            is a hdfs file path to directory where large files are located
	 *            (see foam-data-import)
	 */
	static void calculateHashsums(JavaSparkContext jsc, HbaseConnector hbc, String largeDataHdfsDir) {
		LOGGER.info("Calculate file hashes and write them into HBASE");
		JavaPairRDD<String, byte[]> smallFileHashes = hbc.getSmallFileContent() // -
				.mapValues(Hashing::hashFiles);
		// .mapValues(Hashing::mapToHexString)
		// .take(5).forEach(h -> LOGGER.info("Small File {} hash = {}", h._1, h._2));
		hbc.putHashesToHbase(smallFileHashes);

		JavaPairRDD<String, byte[]> largeFileHashes = getLargeFileContent(jsc, largeDataHdfsDir)
				.mapValues(Hashing::hashFiles);
		// .mapValues(Hashing::mapToHexString)
		// .take(5).forEach(h -> LOGGER.info("Large File {} hash = {}", h._1, h._2));
		hbc.putHashesToHbase(largeFileHashes);
		LOGGER.info("Finished calculation of file hashes and wrote them into HBASE");
	}

	/**
	 * Find duplicate files dependent on the file hashes persisted in HBASE.
	 * 
	 * @param executionContext
	 * @param hbc
	 */
	static void findDuplicateFiles(JavaSparkContext executionContext, HbaseConnector hbc) {
		LOGGER.info("Search for duplicate files:");
		FindDuplicates.filterForDuplicates(hbc.getFileHashAndPath()) // -
				.take(20).forEach(r -> LOGGER.info("Duplicates: {}", r._2()));
	}

	/**
	 * Detect file mime type using Apache Tika. Tika tries to detect the Media-Type
	 * dependent on the magic number within the content and additionally considers
	 * the file extension.
	 * 
	 * @param jsc
	 * @param hbc
	 */
	public static void detectFileMediaType(JavaSparkContext jsc, HbaseConnector hbc, String largeDataHdfsDir) {
		LOGGER.info("Detect file Media-Types and write them into HBASE");
		// Improvement: in both rdds the full file path will be forwarded. But Apache
		// Tike needs at least the file name and extension but not the full file path!.
		// Nevertheless mapping the full path to the small file name could lead to more
		// performance overhead than forwarding the full path.
		JavaPairRDD<String, String> smallFileMediaTypes = hbc.getSmallFileContentWithPath()
				.mapValues(MediaTypeDetection::detectMediaType);
		hbc.putMediaTypesToHbase(smallFileMediaTypes);

		JavaPairRDD<String, String> largeFileMediaTypes = hbc.getOriginalFilePathOfLargeFiles()
				.join(getLargeFileContent(jsc, largeDataHdfsDir))
				.mapValues(MediaTypeDetection::detectMediaTypeDataStream);

		hbc.putMediaTypesToHbase(largeFileMediaTypes);

		// Results can be checked with hbase shell command
		// $ scan 'forensicData', {COLUMNS =>
		// ['metadata:relativeFilePath','metadata:mediaType']}
	}

	/**
	 * * Only for testing purpose! The collect() method can lead to out of memory
	 * exception in driver<br>
	 * Print all available Media Types and the amount of files belonging to them!
	 * 
	 * @param jsc
	 * @param hbc
	 */
	public static void printMediaTypesAndAmount(HbaseConnector hbc) {
		hbc.getMediaTypes()
				.mapToPair((Tuple2<String, String> rowAndMediaType) -> new Tuple2<String, Long>(rowAndMediaType._2, 1L))//
				.reduceByKey((Long c1, Long c2) -> c1 + c2).collect()
				.forEach(e -> LOGGER.info("Media Type = {} ; Occurences = {}", e._1, e._2));
	}

	/**
	 * Only for testing purpose! The collect() method can lead to out of memory
	 * exception in driver<br>
	 * Apache Tika allows to detect the Media-Type also by using the file name
	 * (especially the file extension). Following debug method will print the
	 * difference between media type detection with and without additional
	 * consideration of the file extension. Keep in mind: the file extension itself
	 * could be wrong. But in the most cases the detection with file extension
	 * improves the quality of the media type detection!
	 * 
	 * @param jsc
	 * @param hbc
	 * @param largeDataHdfsDir
	 */
	public static void printDifferentMediaTypes(JavaSparkContext jsc, HbaseConnector hbc, String largeDataHdfsDir) {
		LOGGER.info("Detect file Media-Types with and without file extension and print different results to log file");
		// right outer join doesn't work for hbase files because the ByteBuffers are not
		// serializable! -> therefore an own hbase load (getSmallFileContentWithPath is
		// required!
		JavaPairRDD<String, String> smallFileMediaTypes1 = hbc.getSmallFileContentWithPath()
				// add file path to row id for better output in log file!
				.mapToPair(e -> new Tuple2<String, ByteBuffer>(e._1 + "_" + e._2._1, e._2._2))
				.mapValues(MediaTypeDetection::detectMediaType);
		JavaPairRDD<String, String> smallFileMediaTypes2 = hbc.getSmallFileContentWithPath()
				.mapToPair(e -> new Tuple2<String, Tuple2<String, ByteBuffer>>(e._1 + "_" + e._2._1,
						new Tuple2<String, ByteBuffer>(e._2._1, e._2._2)))
				.mapValues(MediaTypeDetection::detectMediaType);

		smallFileMediaTypes1.join(smallFileMediaTypes2) // -
				.filter(e -> !e._2._1.equals(e._2._2)) // filter for different values
				.collect().forEach(e -> LOGGER.info("{} ; {} ; {}", e._2._1, e._2._2, e._1));

		JavaPairRDD<String, String> largeFileMediaTypes1 = getLargeFileContent(jsc, largeDataHdfsDir)
				.mapValues(MediaTypeDetection::detectMediaType);
		JavaPairRDD<String, String> largeFileMediaTypes2 = hbc.getOriginalFilePathOfLargeFiles()
				// Map full file path to single file name
				.mapValues(fullFilePath -> Paths.get(fullFilePath).getFileName().toString())
				.join(getLargeFileContent(jsc, largeDataHdfsDir))
				.mapValues(MediaTypeDetection::detectMediaTypeDataStream);

		largeFileMediaTypes1.join(largeFileMediaTypes2) // -
				.filter(e -> !e._2._1.equals(e._2._2)) // filter for different values
				.collect().forEach(e -> LOGGER.info("{} ; {} ; {}", e._2._1, e._2._2, e._1));

	}

	private static JavaPairRDD<String, PortableDataStream> getLargeFileContent(JavaSparkContext jsc,
			String largeDataHdfsDir) {
		// Is it possible to load a hdfs file directly with the RDD on executor? It
		// looks like there is no method. So the binaryFiles() method is only available
		// on driver. But otherwise it does make sense, because loading an hdfs on any
		// predefined executor doesn't ensures that this file is really located on the
		// same cluster node (see data locality).
		// Keep in mind binaryFiles() returns the full file path. The file name itself
		// represents the rowKey in HBASE (see foam-data-import project)
		return jsc.binaryFiles(largeDataHdfsDir) // -
				// retrieve row id from hdfs file name
				.mapToPair(e -> new Tuple2<>(new Path(e._1()).getName(), e._2()));
	}
}
