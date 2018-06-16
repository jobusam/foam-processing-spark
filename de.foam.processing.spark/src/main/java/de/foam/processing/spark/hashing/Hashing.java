package de.foam.processing.spark.hashing;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import org.apache.spark.input.PortableDataStream;

/**
 * This class shall calculate all file hashes from a given directory.
 * 
 * @author jobusam
 *
 */
public class Hashing {

	private static final String HASH_ALGORITHM = "SHA-512";

	/**
	 * Calculate File Hashes from given {@link PortableDataStream}
	 * 
	 * @param fileDataStream
	 *            file content as {@link PortableDataStream}
	 * @return hashsum (SHA-512)
	 * @throws IOException
	 * @throws NoSuchAlgorithmException
	 */
	public static byte[] hashFiles(PortableDataStream fileDataStream) throws IOException, NoSuchAlgorithmException {
		MessageDigest digest = MessageDigest.getInstance(HASH_ALGORITHM);
		try (DataInputStream input = fileDataStream.open()) {
			// Create byte array to read data in chunks
			byte[] byteArray = new byte[1024];
			int bytesCount = 0;
			// Read file data and update in message digest
			while ((bytesCount = input.read(byteArray)) != -1) {
				digest.update(byteArray, 0, bytesCount);
			}
		}
		return digest.digest();
	}

	/**
	 * Calculate File Hashes from given {@link ByteBuffer}.
	 * 
	 * @param filesContent
	 *            is the file content as {@link ByteBuffer}.
	 * @return hashsum (SHA-512).
	 * @throws NoSuchAlgorithmException
	 */
	public static byte[] hashFiles(ByteBuffer fileContent) throws NoSuchAlgorithmException {
		MessageDigest digest = MessageDigest.getInstance(HASH_ALGORITHM);
		digest.update(fileContent);
		return digest.digest();
	}

	/**
	 * Convert binary hash into readable HEX representation
	 * 
	 * @param hash
	 * @return
	 */
	public static String mapToHexString(byte[] hash) {
		if (hash == null) {
			return null;
		}
		StringBuffer hexString = new StringBuffer();
		for (int i = 0; i < hash.length; i++) {
			String hex = Integer.toHexString(0xff & hash[i]);
			if (hex.length() == 1)
				hexString.append('0');
			hexString.append(hex);
		}
		return hexString.toString();
	}
}
