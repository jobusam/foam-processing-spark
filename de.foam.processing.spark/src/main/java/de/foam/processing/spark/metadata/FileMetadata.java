package de.foam.processing.spark.metadata;

import java.io.UnsupportedEncodingException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.input.PortableDataStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Tuple2;

/**
 * This class shall retrieve all available Metadata of an HDFS Files. </br>
 * It shall be analysed which Metadata is available within Spark Context and how
 * to access them. Especially the HDFS (XATTR) extended Metadata shall be
 * accessed.
 * 
 * @author jobusam
 *
 */
public class FileMetadata {
	private static final Logger LOGGER = LoggerFactory.getLogger(FileMetadata.class);

	/**
	 * 
	 * @param executionContext
	 *            contains the Spark execution contexts
	 * @param outputDir
	 *            path where to write the text file with the results
	 * @throws Exception
	 */
	public static void extractFileMetadata(JavaSparkContext executionContext, String inputDir, String outputDir)
			throws Exception {
		JavaPairRDD<String, PortableDataStream> filesWithBinContent = executionContext.binaryFiles(inputDir);
		JavaRDD<Tuple2<String, String>> filesWithMetadata = filesWithBinContent.map(filePathWithBinContent -> {
			String fileName = filePathWithBinContent._1();
			FileSystem fileSystem = FileSystem.get(new Configuration());
			Path filePath = new Path(fileName);
			FileStatus status = fileSystem.getFileStatus(filePath);

			StringBuilder builder = new StringBuilder();
			builder.append("Owner =").append(status.getOwner()).append("\n") // -
					.append("Group =").append(status.getGroup()).append("\n")// -
					.append("Permissions =").append(status.getPermission().toString()).append("\n")// -
					.append("File Length =").append(status.getLen()).append("\n")// -
					.append("Block Size =").append(status.getBlockSize()).append("\n")// -
					.append("# Replications =").append(status.getReplication()).append("\n")// -
					.append("Access Time =").append(status.getAccessTime()).append("\n")// -
					.append("Modification Time =").append(status.getModificationTime()).append("\n");

			FileChecksum checksum = fileSystem.getFileChecksum(filePath);
			builder.append(String.format("Checksum (%s) =", checksum.getAlgorithmName())).append(checksum).append("\n");

			// read extended metadata if available
			fileSystem.getXAttrs(filePath).forEach((key, value) -> {
				try {
					builder.append(String.format("XAttr %s =", key)).append(new String(value, "utf8")).append("\n");
				} catch (UnsupportedEncodingException e) {
					LOGGER.error("Can't convert extended attribute values {}", value, e);
				}
			});

			return new Tuple2<String, String>(fileName, builder.toString());
		});

		saveResults(filesWithMetadata, outputDir);
	}

	/**
	 * Save the resulting RDD into one single Text File!
	 * 
	 * @param results
	 */
	public static void saveResults(JavaRDD<Tuple2<String, String>> results, String outputDir) {

		Function<Tuple2<String, String>, String> textFormatter = (Tuple2<String, String> input) -> String
				.format("File Path =%s\n%s\n", input._1(), input._2());

		results.map(textFormatter) // -
				// for an small data set coalesce is ok, but review for large data!!!!
				.coalesce(1)// -
				.saveAsTextFile(outputDir);
	}

}
