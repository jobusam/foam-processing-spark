package de.foam.processing.spark.common;

import org.apache.spark.api.java.JavaPairRDD;

import scala.Tuple2;

/**
 * @author jobusam
 */
public class FindDuplicates {

	/**
	 * Find duplicate files dependent on their hashsum!
	 * 
	 * @param hashesWithFileNames
	 * @return
	 * @throws Exception
	 */
	public static JavaPairRDD<String, String> filterForDuplicates(JavaPairRDD<String, String> hashesWithFileNames)
			throws Exception {
		// Caching could be quite good. But check for large content (a lot of files,
		// with minimum memory)
		hashesWithFileNames.cache();

		JavaPairRDD<String, Long> hashesWithCount = hashesWithFileNames
				.mapToPair((Tuple2<String, String> hashAndFileName) -> new Tuple2<String, Long>(hashAndFileName._1, 1L))//
				.reduceByKey((Long c1, Long c2) -> c1 + c2)//
				.filter((Tuple2<String, Long> hashAndCount) -> hashAndCount._2() > 1);

		JavaPairRDD<String, String> uniqueHashesWithFileNames = hashesWithFileNames
				.reduceByKey((fileNameA, fileNameB) -> fileNameA + ";" + fileNameB);

		JavaPairRDD<String, String> uniqueHashesAndTheirMultipleFileNames = hashesWithCount
				// .join is very expensive considering resource consumption?
				.join(uniqueHashesWithFileNames)
				.mapToPair((Tuple2<String, Tuple2<Long, String>> hashWithCountAndFileList) -> {
					String hash = hashWithCountAndFileList._1;
					String fileList = hashWithCountAndFileList._2._2;
					return new Tuple2<String, String>(hash, fileList);
				});
		return uniqueHashesAndTheirMultipleFileNames;
	}
}
