package de.foam.processing.spark.common;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Tuple2;
import scala.Tuple3;

/**
 * @author jobusam
 */
public class FindDuplicates {

	private static final Logger LOGGER = LoggerFactory.getLogger(FindDuplicates.class);

	/**
	 * Find duplicate files dependent on their hashsum!
	 * 
	 * @param hashesWithFileNames
	 * @return
	 */
	public static JavaPairRDD<String, String> filterForDuplicates(JavaPairRDD<String, String> hashesWithFileNames) {
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

	/**
	 * Find duplicate files dependent on their hashsum!
	 * 
	 * @param filePathsAndHashes
	 *            {@link JavaRDD}<br>
	 *            val_1 = contains the row ID of the the table "forensicData"<br>
	 *            val_2 = contains the relativeFilePath of the file <br>
	 *            val_3 = contains the file hash of the file
	 * @return JavaPairRDD val_1 = contains the unique file hash of the file<br>
	 *         val_2 = list of files e.g "row0_[path_name];row3_[path_name]"...<br>
	 */
	public static JavaPairRDD<String, String> filterForDuplicates(
			JavaRDD<Tuple3<String, String, String>> filePathsAndHashes) {
		// Caching could be quite good. But check for large content (a lot of files,
		// with minimum memory)
		filePathsAndHashes.cache();

		// Caution: Keep in mind reduceByKey() method works correct with Strings but not
		// with byte[]! Therefore the hashes should be converted into hex format
		// previously!
		JavaPairRDD<String, Long> hashesWithCount = filePathsAndHashes.mapToPair(
				(Tuple3<String, String, String> filePathAndHash) -> new Tuple2<String, Long>(filePathAndHash._3(), 1L))//
				.reduceByKey((Long c1, Long c2) -> c1 + c2)//
				.filter((Tuple2<String, Long> hashAndCount) -> hashAndCount._2() > 1);

		JavaPairRDD<String, String> uniqueHashesWithIdAndName = filePathsAndHashes
				.mapToPair(filePathAndHash -> new Tuple2<String, String>(filePathAndHash._3(),
						filePathAndHash._1() + "_" + filePathAndHash._2()))
				.reduceByKey((fileNameA, fileNameB) -> fileNameA + ";" + fileNameB);

		JavaPairRDD<String, String> uniqueHashesAndTheirMultipleFileNames = hashesWithCount
				// .join is very expensive considering resource consumption?
				.join(uniqueHashesWithIdAndName)
				.mapToPair((Tuple2<String, Tuple2<Long, String>> hashWithCountAndFileList) -> {
					return new Tuple2<String, String>(hashWithCountAndFileList._1(),
							hashWithCountAndFileList._2()._2());
				});
		return uniqueHashesAndTheirMultipleFileNames;
	}
}
