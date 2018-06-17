package de.foam.processing.spark.common;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

import org.apache.spark.input.PortableDataStream;
import org.apache.tika.config.TikaConfig;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.metadata.Metadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Tuple2;

/**
 * Detecting Media Types using Apache Tika.
 * 
 * @author jobusam
 */
public class MediaTypeDetection {
	private static final Logger LOGGER = LoggerFactory.getLogger(MediaTypeDetection.class);

	/**
	 * Detect Media Type depending on file content and additionally considers the
	 * file extension.
	 * 
	 * @param fileContent
	 *            val_1 = filePath (inclusive file extension)<br>
	 *            val_2 = content as ByteBuffer
	 * @return MediaType
	 */
	public static String detectMediaType(Tuple2<String, ByteBuffer> fileContent) {
		return detectMediaType(fileContent._1, new ByteBufferBasedInputStream(fileContent._2));
	}

	/**
	 * Detect Media Type depending on file content and additionally considers the
	 * file extension. Same method like {@link #detectMediaType(Tuple2)} but uses
	 * {@link PortableDataStream} instead {@link ByteBuffer}.
	 * 
	 * @param fileContent
	 *            val_1 = filePath (inclusive file extension)<br>
	 *            val_2 = content as {@link PortableDataStream}
	 * @return MediaType
	 */
	public static String detectMediaTypeDataStream(Tuple2<String, PortableDataStream> fileContent) {
		return detectMediaType(fileContent._1, fileContent._2.open());
	}

	/**
	 * Detect Media Type depending on file content
	 * 
	 * @param fileContent
	 *            as ByteBuffer
	 * @return MediaType
	 */
	public static String detectMediaType(ByteBuffer fileContent) {
		return detectMediaType(new Metadata(), new ByteBufferBasedInputStream(fileContent));
	}

	/**
	 * Same like {@link #detectMediaType(ByteBuffer)} but with
	 * {@link PortableDataStream} instead.
	 * 
	 * @param fileContent
	 * @return MediaType
	 */
	public static String detectMediaType(PortableDataStream fileContent) {
		return detectMediaType(new Metadata(), fileContent.open());
	}

	private static String detectMediaType(String fileName, InputStream input) {
		// set file name as context variable
		Metadata m = new Metadata();
		m.set(Metadata.RESOURCE_NAME_KEY, fileName);
		return detectMediaType(m, input);
	}

	private static String detectMediaType(Metadata metadata, InputStream input) {
		// Use AutoClosable for closing Stream afterwards!
		try (TikaInputStream tikaInput = TikaInputStream.get(input)) {
			return TikaConfig.getDefaultConfig().getDetector().detect(tikaInput, metadata).toString();
		} catch (IOException e) {
			LOGGER.warn("Can't detect media type", e);
			return null;
		}
	}

}
