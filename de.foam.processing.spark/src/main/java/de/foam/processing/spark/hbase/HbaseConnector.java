package de.foam.processing.spark.hbase;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Optional;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueExcludeFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.spark.JavaHBaseContext;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.foam.processing.spark.hashing.Hashing;
import scala.Tuple2;
import scala.Tuple3;

/**
 * This class creates RDDs based on the content persisted in HBASE and writes
 * data to HBASE. The data model in HBASE is derived from foam-data-import
 * project.<br>
 * 
 * @author jobusam
 * 
 * @see <a href=
 *      "https://github.com/jobusam/foam-data-import">foam-data-import</a>
 * @see <a href=
 *      "https://github.com/apache/hbase/blob/master/hbase-spark/src/main/java/org/apache/hadoop/hbase/spark/example/hbasecontext/JavaHBaseBulkGetExample.java">JavaHBaseBulkGetExample.java</a>
 * 
 */
public class HbaseConnector {
	private static final String HBASE_ZOOKEEPER_PROPERTY_CLIENT_PORT = "hbase.zookeeper.property.clientPort";

	private static final String HBASE_ZOOKEEPER_QUORUM = "hbase.zookeeper.quorum";

	private static final Logger LOGGER = LoggerFactory.getLogger(HbaseConnector.class);

	// local Standalone config to access hbase
	private static final String HBASE_STANDALONE_HOST = "localhost";
	private static final String HBASE_PORT = "2181";

	// table name, column families and column names in HBASE
	private static final TableName HBASE_FORENSIC_TABLE = TableName.valueOf("forensicData");

	// cf metadata and including columns
	private static final byte[] CF_METADATA = Bytes.toBytes("metadata");
	private static final byte[] C_FILE_PATH = Bytes.toBytes("relativeFilePath");
	private static final byte[] C_FILE_TYPE = Bytes.toBytes("fileType");
	private static final byte[] C_FILE_SIZE = Bytes.toBytes("fileSize");
	private static final byte[] C_OWNER = Bytes.toBytes("owner");
	private static final byte[] C_GROUP = Bytes.toBytes("group");
	private static final byte[] C_PERMISSIONS = Bytes.toBytes("permissions");
	private static final byte[] C_LAST_MODIFIED = Bytes.toBytes("lastModified");
	private static final byte[] C_LAST_CHANGED = Bytes.toBytes("lastChanged");
	private static final byte[] C_LAST_ACCESSED = Bytes.toBytes("lastAccessed");
	private static final byte[] C_CREATED = Bytes.toBytes("created");
	private static final byte[] C_FILE_HASH = Bytes.toBytes("fileHash");
	private static final byte[] C_MEDIA_TYPE = Bytes.toBytes("mediaType");

	// cf content and including columns
	private static final byte[] CF_CONTENT = Bytes.toBytes("content");
	private static final byte[] C_FILE_CONTENT = Bytes.toBytes("fileContent");
	private static final byte[] C_HDFS_FILE_PATH = Bytes.toBytes("hdfsFilePath");

	private final JavaHBaseContext hbaseContext;

	/**
	 * Create a JavaHBASEContext to access HBASE/Zookeeper.
	 * 
	 * @param jsc
	 *            is the JavaSparkContext
	 * @param hbaseConfigFile
	 *            Optional<Path> to hbase-site.xml. Used to access HBASE.
	 */
	public HbaseConnector(JavaSparkContext jsc, Optional<Path> hbaseConfigFile) {
		Configuration conf = HBaseConfiguration.create();
		if (hbaseConfigFile.isPresent()) {
			LOGGER.info("Use configuration file {} to connect to HBASE", hbaseConfigFile.get());
			conf.addResource(new org.apache.hadoop.fs.Path(hbaseConfigFile.get().toString()));
		} else {
			LOGGER.info("Use default configuration to connect to HBASE");
			// this works only for HBASE Standalone
			conf.set(HBASE_ZOOKEEPER_QUORUM, HBASE_STANDALONE_HOST);
			conf.set(HBASE_ZOOKEEPER_PROPERTY_CLIENT_PORT, HBASE_PORT);
			// conf.set("hbase.client.nonces.enabled", "false");
		}
		LOGGER.info("Connection: <{} = {}>, <{} = {}>", // -
				HBASE_ZOOKEEPER_QUORUM, conf.get(HBASE_ZOOKEEPER_QUORUM), // -
				HBASE_ZOOKEEPER_PROPERTY_CLIENT_PORT, conf.get(HBASE_ZOOKEEPER_PROPERTY_CLIENT_PORT));
		hbaseContext = new JavaHBaseContext(jsc, conf);
	}

	/**
	 * Create a {@link JavaRDD} that contains a {@link Metadata} object for every
	 * entry in HBASE Table "forensicData".
	 */
	public JavaRDD<Metadata> getForensicMetadata() {
		Scan scans = new Scan().addFamily(CF_METADATA);
		// .withStartRow(Bytes.toBytes("row1")).withStopRow(Bytes.toBytes("row20"),true);
		// FirstKeyFilter only retrieves the row keys, but doesn't request any column!
		// scans.setFilter(new FirstKeyOnlyFilter());
		return hbaseContext.hbaseRDD(HBASE_FORENSIC_TABLE, scans, r -> r._2).map(resultToMetadata);
	}

	Function<Result, Metadata> resultToMetadata = (result) -> {
		return new Metadata(Bytes.toString(result.getRow()), // -
				Bytes.toString(result.getValue(CF_METADATA, C_FILE_PATH)),
				Bytes.toString(result.getValue(CF_METADATA, C_FILE_TYPE)),
				convertFileSize(result.getValue(CF_METADATA, C_FILE_SIZE)),
				Bytes.toString(result.getValue(CF_METADATA, C_OWNER)),
				Bytes.toString(result.getValue(CF_METADATA, C_GROUP)),
				Bytes.toString(result.getValue(CF_METADATA, C_PERMISSIONS)),
				Bytes.toString(result.getValue(CF_METADATA, C_LAST_MODIFIED)),
				Bytes.toString(result.getValue(CF_METADATA, C_LAST_CHANGED)),
				Bytes.toString(result.getValue(CF_METADATA, C_LAST_ACCESSED)),
				Bytes.toString(result.getValue(CF_METADATA, C_CREATED)));
	};

	static long convertFileSize(byte[] binaryFileSize) {
		if (binaryFileSize != null && binaryFileSize.length > 0) {
			return Long.valueOf(Bytes.toString(binaryFileSize));
		}
		return 0L;
	}

	/**
	 * Create a {@link JavaRDD} that contains a {@link Content} object for every
	 * entry in HBASE Table "forensicData". This method reads several values from
	 * different cells...
	 * 
	 * @return the content contains for the entry either a raw {@link ByteBuffer}
	 *         (small files) or an hdfs file path! Additionally the original file
	 *         path and the row id are included.
	 */
	public JavaRDD<Content> getForensicFileContent() {

		Scan scans = new Scan().addFamily(CF_CONTENT)
				// request required columns from metadata cf
				.addColumn(CF_METADATA, C_FILE_PATH)// .addColumn(CF_METADATA, C_FILE_TYPE)
				// request only data files (no directories or links)
				// and exclude the file type column itself (performance optimization)
				.setFilter(dataFilesOnly());
		return hbaseContext.hbaseRDD(HBASE_FORENSIC_TABLE, scans, r -> r._2).map(resultToContent);
	}

	Function<Result, Content> resultToContent = (result) -> {
		return new Content(Bytes.toString(result.getRow()), // -
				Bytes.toString(result.getValue(CF_METADATA, C_FILE_PATH)),
				Bytes.toString(result.getValue(CF_CONTENT, C_HDFS_FILE_PATH)),
				result.getValueAsByteBuffer(CF_CONTENT, C_FILE_CONTENT));
	};

	/**
	 * Create a {@link JavaRDD} that reads the values from the column
	 * content:fileContent within the HBASE table "forensicData". Keep in mind large
	 * files are directly saved in HDFS and won't be requested by this method. To
	 * get the large files see {@link #getLargeFileContent()}.<br>
	 * CAUTION: This method retrieves only files with non-empty content. Data files
	 * with empty content are skipped!
	 * 
	 * @return {@link JavaPairRDD}<br>
	 *         val_1 = contains the row ID of the HBASE entry<br>
	 *         val_2 = file content of small files as {@link ByteBuffer}.
	 * 
	 */
	public JavaPairRDD<String, ByteBuffer> getSmallFileContent() {
		// request only files that are persisted in hbase (hdfsFilePath != null)
		Scan scans = new Scan().addColumn(CF_CONTENT, C_FILE_CONTENT)
				.setFilter(valueIsNotNull(CF_CONTENT, C_FILE_CONTENT));

		return hbaseContext.hbaseRDD(HBASE_FORENSIC_TABLE, scans, r -> r._2).map(result -> {
			return new Tuple2<String, ByteBuffer>(Bytes.toString(result.getRow()),
					result.getValueAsByteBuffer(CF_CONTENT, C_FILE_CONTENT));
		}).mapToPair(t -> t);
	}

	/**
	 * Same method like {@link #getSmallFileContent()} but will additionally request
	 * the file path of the entry.
	 * 
	 * @return {@link JavaPairRDD}<br>
	 *         val_1 = contains the row ID of the HBASE entry<br>
	 *         val_2 = {@link Tuple2}<br>
	 *         ------ val_1 = file path ("metadata:relativeFilePath")<br>
	 *         ------ val_2 = file content of small files as {@link ByteBuffer}.
	 */
	public JavaPairRDD<String, Tuple2<String, ByteBuffer>> getSmallFileContentWithPath() {
		// request only files that are persisted in hbase (hdfsFilePath != null)
		Scan scans = new Scan().addColumn(CF_CONTENT, C_FILE_CONTENT).addColumn(CF_METADATA, C_FILE_PATH)
				.setFilter(valueIsNotNull(CF_CONTENT, C_FILE_CONTENT));

		return hbaseContext.hbaseRDD(HBASE_FORENSIC_TABLE, scans, r -> r._2).map(result -> {
			return new Tuple2<String, Tuple2<String, ByteBuffer>>(Bytes.toString(result.getRow()),
					new Tuple2<String, ByteBuffer>(Bytes.toString(result.getValue(CF_METADATA, C_FILE_PATH)),
							result.getValueAsByteBuffer(CF_CONTENT, C_FILE_CONTENT)));
		}).mapToPair(t -> t);
	}

	/**
	 * Following method is useless! Because it's not possible to access the file
	 * content of a given file path within any executor without using the spark
	 * context!
	 * 
	 * Create a {@link JavaRDD} that reads the values from the column
	 * content:hdfsFilePath within the HBASE table "forensicData". This values are
	 * file paths refering to the original large file content in HDFS. This file
	 * content will be returned. (See also {@link #getSmallFileContent() to request
	 * files that are directly saved in HBASE)
	 * 
	 * @return {@link JavaPairRDD}<br>
	 *         val_1 = contains the row ID of the HBASE entry<br>
	 *         val_2 = HDFS file path {@link org.apache.hadoop.fs.Path}.
	 */
	public JavaPairRDD<String, org.apache.hadoop.fs.Path> getLargeFileContent() {

		// request only files that are persisted in hbase (hdfsFilePath != null)
		Scan scans = new Scan().addColumn(CF_CONTENT, C_HDFS_FILE_PATH)
				.setFilter(valueIsNotNull(CF_CONTENT, C_HDFS_FILE_PATH));

		return hbaseContext.hbaseRDD(HBASE_FORENSIC_TABLE, scans, r -> r._2).map(result -> {
			return new Tuple2<String, org.apache.hadoop.fs.Path>(Bytes.toString(result.getRow()),
					new org.apache.hadoop.fs.Path(Bytes.toString(result.getValue(CF_CONTENT, C_HDFS_FILE_PATH))));
		}).mapToPair(t -> t);
	}

	/**
	 * Request all original file paths of large files only! The difference between a
	 * large and a small file is that the large file content itself is persisted in
	 * HDFS. Therefore a large file entry in HBASE additionally have a column
	 * "content:hdfsFilePath". This method retrieves all original file paths
	 * ("metadata:relativeFilePath") of large files.
	 * 
	 * @return {@link JavaPairRDD}<br>
	 *         val_1 = contains the row ID of the HBASE entry<br>
	 *         val_2 = original file path.
	 */
	public JavaPairRDD<String, String> getOriginalFilePathOfLargeFiles() {
		// request only files that are persisted in hbase (hdfsFilePath != null)
		Scan scans = new Scan().addColumn(CF_METADATA, C_FILE_PATH)
				.setFilter(valueIsNotNullExclusive(CF_CONTENT, C_HDFS_FILE_PATH));

		return hbaseContext.hbaseRDD(HBASE_FORENSIC_TABLE, scans, r -> r._2).map(result -> {
			return new Tuple2<String, String>(Bytes.toString(result.getRow()),
					Bytes.toString(result.getValue(CF_METADATA, C_FILE_PATH)));
		}).mapToPair(t -> t);
	}

	/**
	 * Create a {@link JavaRDD} that reads the file hashes and file paths from
	 * HBASE. Use table "forensicData". Enties without hash will be skipped! So
	 * ensure that the hashes are already persisted in HBASE
	 * 
	 * @return {@link JavaRDD}<br>
	 *         val_1 = contains the row ID of the the table "forensicData"<br>
	 *         val_2 = contains the relativeFilePath of the file (column =
	 *         metadata:relativeFilePath) val_3 = contains the file hash of the file
	 *         (column = metadata:fileHash).
	 * 
	 */
	public JavaRDD<Tuple3<String, String, String>> getFileHashAndPath() {
		// request only files that have any hash value
		Scan scans = new Scan().addFamily(CF_METADATA)
				// .addColumn(CF_METADATA, C_FILE_PATH).addColumn(CF_METADATA, C_FILE_HASH)
				.setFilter(valueIsNotNull(CF_METADATA, C_FILE_HASH));

		return hbaseContext.hbaseRDD(HBASE_FORENSIC_TABLE, scans, r -> r._2).map(result -> {
			return new Tuple3<String, String, String>(Bytes.toString(result.getRow()),
					Bytes.toString(result.getValue(CF_METADATA, C_FILE_PATH)),
					Hashing.mapToHexString(result.getValue(CF_METADATA, C_FILE_HASH)));
		});
	}

	/**
	 * Get Media Types from HBASE. Use table "forensicMetadata" and column
	 * "metadata:mediaType"
	 * 
	 * @return {@link JavaPairRDD}<br>
	 *         val_1 = contains the row ID of the HBASE entry<br>
	 *         val_2 = mediaType
	 */
	public JavaPairRDD<String, String> getMediaTypes() {
		// request only files that are persisted in hbase (hdfsFilePath != null)
		Scan scans = new Scan().addColumn(CF_METADATA, C_MEDIA_TYPE)
				.setFilter(valueIsNotNull(CF_METADATA, C_MEDIA_TYPE));

		return hbaseContext.hbaseRDD(HBASE_FORENSIC_TABLE, scans, r -> r._2).map(result -> {
			return new Tuple2<String, String>(Bytes.toString(result.getRow()),
					Bytes.toString(result.getValue(CF_METADATA, C_MEDIA_TYPE)));
		}).mapToPair(t -> t);
	}

	/**
	 * Write file hashes to HBASE. Put the values into table "forensicMetadata" and
	 * column "metadata:fileHash"
	 * 
	 * @param {@link
	 * 			JavaPairRDD}<br>
	 *            val_1 = contains the row ID of the HBASE entry<br>
	 *            val_2 = file hash.
	 */
	public void putHashesToHbase(JavaPairRDD<String, byte[]> fileHashes) {
		hbaseContext.bulkPut(JavaPairRDD.toRDD(fileHashes).toJavaRDD(), HBASE_FORENSIC_TABLE, (entry) -> {
			return new Put(Bytes.toBytes(entry._1())).addColumn(CF_METADATA, C_FILE_HASH, entry._2());
		});
	}

	/**
	 * Write file media types to HBASE. Put the values into table "forensicMetadata"
	 * and column "metadata:mediaType"
	 * 
	 * @param {@link
	 * 			JavaPairRDD}<br>
	 *            val_1 = contains the row ID of the HBASE entry<br>
	 *            val_2 = mediaType
	 */
	public void putMediaTypesToHbase(JavaPairRDD<String, String> fileMediaTypes) {
		hbaseContext.bulkPut(JavaPairRDD.toRDD(fileMediaTypes).toJavaRDD(), HBASE_FORENSIC_TABLE, (entry) -> {
			return new Put(Bytes.toBytes(entry._1())).addColumn(CF_METADATA, C_MEDIA_TYPE, Bytes.toBytes(entry._2()));
		});
	}

	/**
	 * Create a filter object that skips the whole row, if the specified column
	 * value is empty.
	 */
	private static SingleColumnValueFilter valueIsNotNull(byte[] family, byte[] qualifier) {
		// For HDP 2.6.3 and HBASE 1.1.2 following constructor is correct.
		SingleColumnValueFilter filter = new SingleColumnValueFilter(family, qualifier, // -
				CompareFilter.CompareOp.NOT_EQUAL, // for HBASE 2.0.0 and later CompareOperator.NOT_EQUAL is preferred!
				Bytes.toBytes(""));
		// Otherwise rows without the specified column will emitted!
		filter.setFilterIfMissing(true);
		return filter;
	}

	/**
	 * Create a filter object that skips the whole row, if the specified column
	 * value is empty. Additionally this column will also excluded if the value is
	 * available! See {@link SingleColumnValueExcludeFilter}!
	 */
	private static SingleColumnValueFilter valueIsNotNullExclusive(byte[] family, byte[] qualifier) {
		// For HDP 2.6.3 and HBASE 1.1.2 following constructor is correct.
		SingleColumnValueExcludeFilter filter = new SingleColumnValueExcludeFilter(family, qualifier, // -
				CompareFilter.CompareOp.NOT_EQUAL, // for HBASE 2.0.0 and later CompareOperator.NOT_EQUAL is preferred!
				Bytes.toBytes(""));
		// Otherwise rows without the specified column will emitted!
		filter.setFilterIfMissing(true);
		return filter;
	}

	/**
	 * Filter the rows and retrieve only data files! (Directories, Symbolic Links
	 * and Other Files will skipped! If the row doesn't contain the column
	 * "metadata:fileType" it will be also skipped!
	 */
	private static SingleColumnValueFilter dataFilesOnly() {
		// For HDP 2.6.3 and HBASE 1.1.2 following constructor is correct.
		SingleColumnValueExcludeFilter filter = new SingleColumnValueExcludeFilter(CF_METADATA, C_FILE_TYPE, // -
				CompareFilter.CompareOp.EQUAL, //// for HBASE 2.0.0 and later CompareOperator.EQUAL is preferred!
				Bytes.toBytes("DATA_FILE"));
		// Otherwise rows without the specified column will emitted!
		filter.setFilterIfMissing(true);
		return filter;
	}
}
