package de.foam.processing.spark;

import java.nio.file.Paths;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.input.PortableDataStream;

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

	// private static String dataDir = "/mnt/ssd/apps/data";
	private static String dataDir = "/data";

	public static void main(String[] args) {

		if (args.length == 1 && args[0] != null && Paths.get(args[0]).toFile().isDirectory()) {
			dataDir = args[0];
		}
		System.out.println(String.format("Data directory for analysis is %s", dataDir));

		JavaSparkContext executionContext = createExecutionContext();

		JavaPairRDD<String, PortableDataStream> filesWithBinContent = executionContext.binaryFiles(dataDir);
		System.out
				.println(String.format("Num of Partitions for execution = %d", filesWithBinContent.getNumPartitions()));

		try {
			JavaPairRDD<String, String> hashesWithFileNames = Hashing.hashFiles(filesWithBinContent);
			JavaPairRDD<String, String> results = FindDuplicates.filterForDuplicates(hashesWithFileNames);
			printResults(results);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		executionContext.stop();

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
