package de.foam.processing.spark.datamodel;

import java.io.Serializable;

/**
 * This class represents a HBASE row entry of the "forensicData" Table.
 * 
 * @author jobusam
 *
 */
public class Entry implements Serializable {
	private static final long serialVersionUID = 1L;
	private String rowID;
	private Metadata metadata;

	public Entry(String rowID, Metadata metadata) {
		super();
		this.rowID = rowID;
		this.metadata = metadata;
	}

	public String getRowID() {
		return rowID;
	}

	public Metadata getMetadata() {
		return metadata;
	}

}
