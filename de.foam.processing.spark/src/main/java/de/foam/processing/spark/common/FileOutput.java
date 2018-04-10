package de.foam.processing.spark.common;

import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;

import scala.Tuple2;

/**
 * Support functions for displaying/saving RDD content
 * 
 * @author jobusam
 *
 */
public class FileOutput {
	/**
	 * Save the resulting RDD into one single Text File!
	 * 
	 * @param results
	 */
	public static void saveResults(JavaPairRDD<String, String> results, String outputDir) {

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

	/**
	 * Print output directly to locale console. Caution this works only for spark
	 * app execution in client mode or standalone mode with local console available.
	 * In cluster mode there is no chance to see the console output of the worker
	 * instance!
	 * 
	 * @param results
	 */
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
