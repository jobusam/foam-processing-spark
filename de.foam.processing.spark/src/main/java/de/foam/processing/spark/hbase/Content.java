package de.foam.processing.spark.hbase;

import java.io.Serializable;
import java.nio.ByteBuffer;

/**
 * This class represents the metadata of a given file saved in the HBASE table
 * "forensicData".
 * 
 * @author jobusam
 *
 */
public class Content implements Serializable {
	private static final long serialVersionUID = 1L;
	private String id;
	private String relativeFilePath;
	private String hdfsFilePath;
	private ByteBuffer content;

	public Content(String id, String relativeFilePath, String hdfsFilePath, ByteBuffer content) {
		this.id = id;
		this.relativeFilePath = relativeFilePath;
		this.hdfsFilePath = hdfsFilePath;
		this.content = content;
	}

	public String getId() {
		return id;
	}

	public String getRelativeFilePath() {
		return relativeFilePath;
	}

	public String getHdfsFilePath() {
		return hdfsFilePath;
	}

	public ByteBuffer getContent() {
		return content;
	}

}
