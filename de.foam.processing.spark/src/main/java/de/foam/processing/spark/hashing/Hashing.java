package de.foam.processing.spark.hashing;

import java.io.DataInputStream;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.input.PortableDataStream;

import scala.Tuple2;

/**
 * This class shall calculate all file hashes from a given directory.
 * 
 * @author jobusam
 *
 */
public class Hashing {

	/**
	 * Calculate File Hashes from given files.
	 * 
	 * @param filesWithBinContent
	 *            contains filePath and a {@link PortableDataStream} that can be
	 *            read to get the file content
	 * @return a {@link JavaPairRDD} that contains the hashsum (SHA-512) as key and
	 *         the filePath as content!
	 */
	public static JavaPairRDD<String, String> hashFiles(JavaPairRDD<String, PortableDataStream> filesWithBinContent)
			throws Exception {

		JavaPairRDD<String, String> hashesWithFileNames = filesWithBinContent
				.mapToPair((Tuple2<String, PortableDataStream> fileNameAndContent) -> {
					String fileName = fileNameAndContent._1;

					MessageDigest digest = MessageDigest.getInstance("SHA-512");
					try (DataInputStream input = fileNameAndContent._2.open()) {
						// Create byte array to read data in chunks
						byte[] byteArray = new byte[1024];
						int bytesCount = 0;

						// Read file data and update in message digest
						while ((bytesCount = input.read(byteArray)) != -1) {
							digest.update(byteArray, 0, bytesCount);
						}
						;
					}
					byte[] hash = digest.digest();
					StringBuffer hexString = new StringBuffer();
					for (int i = 0; i < hash.length; i++) {
						String hex = Integer.toHexString(0xff & hash[i]);
						if (hex.length() == 1)
							hexString.append('0');
						hexString.append(hex);
					}
					// Switch fileName and hash, to be able to find duplicated files by reduceByKey
					return new Tuple2<String, String>(hexString.toString(), fileName);
				});
		return hashesWithFileNames;
	}

	public static JavaPairRDD<String, String> hashStrings(JavaPairRDD<String, String> filesWithContent)
			throws Exception {

		JavaPairRDD<String, String> hashesWithFileNames = filesWithContent
				.mapToPair((Tuple2<String, String> fileNameAndContent) -> {
					String fileName = fileNameAndContent._1;
					String content = fileNameAndContent._2;

					MessageDigest digest = MessageDigest.getInstance("SHA-512");
					byte[] hash = digest.digest(content.getBytes(StandardCharsets.UTF_8));

					StringBuffer hexString = new StringBuffer();
					for (int i = 0; i < hash.length; i++) {
						String hex = Integer.toHexString(0xff & hash[i]);
						if (hex.length() == 1)
							hexString.append('0');
						hexString.append(hex);
					}
					// Switch fileName and hash, to be able to find duplicated files by reduceByKey
					return new Tuple2<String, String>(hexString.toString(), fileName);
				});
		return hashesWithFileNames;
	}
}
