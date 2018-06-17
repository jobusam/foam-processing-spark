package de.foam.processing.spark.common;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

/**
 * Wrap a ByteBuffer into an InputStream. See also examples in web. Used for
 * wrap small files from HBASE into an {@link InputStream} required by Apache
 * Tika for Media Type detection.
 * 
 * @author jobusam
 */
public class ByteBufferBasedInputStream extends InputStream {
	ByteBuffer buffer;

	ByteBufferBasedInputStream(ByteBuffer buffer) {
		this.buffer = buffer;
	}

	public synchronized int read() throws IOException {
		if (!buffer.hasRemaining()) {
			return -1;
		}
		return buffer.get();
	}

	public synchronized int read(byte[] bytes, int off, int len) throws IOException {
		len = Math.min(len, buffer.remaining());
		buffer.get(bytes, off, len);
		return len;
	}
}