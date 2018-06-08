package de.foam.processing.spark.hbase;

import java.io.Serializable;

/**
 * This class represents the metadata of a given file saved in the HBASE table
 * "forensicData".
 * 
 * @author jobusam
 *
 */
public class Metadata implements Serializable {
	private static final long serialVersionUID = 1L;
	private String id;
	private String relativeFilePath;
	private String fileType;
	private long fileSize;
	private String owner;
	private String group;
	private String permissions;
	private String lastModified;
	private String lastChanged;
	private String lastAccessed;
	private String created;

	public Metadata(String id, String relativeFilePath, String fileType, long fileSize, String owner, String group,
			String permissions, String lastModified, String lastChanged, String lastAccessed, String created) {
		this.id = id;
		this.relativeFilePath = relativeFilePath;
		this.fileType = fileType;
		this.fileSize = fileSize;
		this.owner = owner;
		this.group = group;
		this.permissions = permissions;
		this.lastModified = lastModified;
		this.lastChanged = lastChanged;
		this.lastAccessed = lastAccessed;
		this.created = created;
	}

	public String getId() {
		return id;
	}

	public String getRelativeFilePath() {
		return relativeFilePath;
	}

	public String getFileType() {
		return fileType;
	}

	public long getFileSize() {
		return fileSize;
	}

	public String getOwner() {
		return owner;
	}

	public String getGroup() {
		return group;
	}

	public String getPermissions() {
		return permissions;
	}

	public String getLastModified() {
		return lastModified;
	}

	public String getLastChanged() {
		return lastChanged;
	}

	public String getLastAccessed() {
		return lastAccessed;
	}

	public String getCreated() {
		return created;
	}
}
