package de.foam.processing.spark;

import java.nio.file.Paths;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.input.PortableDataStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.foam.processing.spark.common.FindDuplicates;
import de.foam.processing.spark.hashing.Hashing;
import scala.Tuple2;

/**
 * This Application calculates file hashes from a given directory and prints any
 * duplicate files to local console. At the moment the results will be printed
 * to local console. Therefore it works only for submitting the application on a
 * local Apache Spark standalone instance.
 * 
 * @author jobusam
 *
 */
public class ForensicAnalysis {

	private static String dataDir;
	private static String outputDir;
	private static final Logger LOGGER = LoggerFactory.getLogger(ForensicAnalysis.class);

	public static void main(String[] args) {

		boolean validParams = validateInputParams(args);
		if (!validParams) {
			System.exit(1);
		}

		JavaSparkContext executionContext = createExecutionContext();

		JavaPairRDD<String, PortableDataStream> filesWithBinContent = executionContext.binaryFiles(dataDir);
		System.out
				.println(String.format("Num of Partitions for execution = %d", filesWithBinContent.getNumPartitions()));

		try {
			JavaPairRDD<String, String> hashesWithFileNames = Hashing.hashFiles(filesWithBinContent);
			JavaPairRDD<String, String> results = FindDuplicates.filterForDuplicates(hashesWithFileNames);
			// printResults(results);
			saveResults(results);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

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

	/**
	 * Save the resulting RDD into one single Text File!
	 * 
	 * @param results
	 */
	public static void saveResults(JavaPairRDD<String, String> results) {

		Function<Tuple2<String, String>, String> textFormatter = (Tuple2<String, String> input) -> {
			String head = String.format("SHA512-Hash: %s \n", input._1());
			String body = input._2().replace(";", "\n");
			return head + body + "\n\n";
		};

		results.map(textFormatter) // -
				// for an small data set coalesce is ok, but review for large data!!!!
				.coalesce(1)// -
				.saveAsTextFile(outputDir);
	}

	public static void printResults(JavaPairRDD<String, String> results) {
		// For large sets this collection costs a lot of ressources!!!!!
		// or use following command to limit the amount of items on driver node!
		// results.take(10).forEach(System.out::println);

		List<Tuple2<String, String>> asList = results.collect();

		for (Tuple2<String, String> element : asList) {
			String hash = element._1;
			String fileList = element._2;

			System.out.println("========================================");
			System.out.println(hash);
			java.util.Arrays.asList(fileList.split(";")).forEach(System.out::println);
			System.out.println();
		}
	}
}
