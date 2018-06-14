package de.foam.processing.spark.hbase;

import java.nio.file.Path;
import java.util.Optional;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.SingleColumnValueExcludeFilter;
import org.apache.hadoop.hbase.spark.JavaHBaseContext;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class creates RDDs based on the content persisted in HBASE. The data
 * model in HBASE is derived from foam-data-import project.<br>
 * 
 * @author jobusam
 * 
 * @see <a href=
 *      "https://github.com/jobusam/foam-data-import">foam-data-import</a>
 */
public class HbaseRead {
	private static final Logger LOGGER = LoggerFactory.getLogger(HbaseRead.class);

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
	public HbaseRead(JavaSparkContext jsc, Optional<Path> hbaseConfigFile) {
		Configuration conf = HBaseConfiguration.create();
		if (hbaseConfigFile.isPresent()) {
			LOGGER.info("Use configuration file {} to connect to HBASE", hbaseConfigFile.get());
			conf.addResource(new org.apache.hadoop.fs.Path(hbaseConfigFile.get().toString()));
		} else {
			LOGGER.info("Use default configuration to connect to HBASE: <hbase.zookeeper.quorum = {}>, "
					+ "<hbase.zookeeper.property.clientPort = {}>", HBASE_STANDALONE_HOST, HBASE_PORT);
			// this works only for HBASE Standalone
			conf.set("hbase.zookeeper.quorum", HBASE_STANDALONE_HOST);
			conf.set("hbase.zookeeper.property.clientPort", HBASE_PORT);
		}
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
	 * entry in HBASE Table "forensicData".
	 */
	public JavaRDD<Content> getForensicFileContent(JavaSparkContext jsc) {
		Scan scans = new Scan().addFamily(CF_CONTENT)
				// request required columns from metadata cf
				.addColumn(CF_METADATA, C_FILE_PATH)// .addColumn(CF_METADATA, C_FILE_TYPE)
				// request only data files (no directories or links)
				// and exclude the file type column itself (performance optimization)
				.setFilter(new SingleColumnValueExcludeFilter(CF_METADATA, C_FILE_TYPE, CompareOperator.EQUAL,
						Bytes.toBytes("DATA_FILE")));
		return hbaseContext.hbaseRDD(HBASE_FORENSIC_TABLE, scans, r -> r._2).map(resultToContent);
	}

	Function<Result, Content> resultToContent = (result) -> {
		return new Content(Bytes.toString(result.getRow()), // -
				Bytes.toString(result.getValue(CF_METADATA, C_FILE_PATH)),
				Bytes.toString(result.getValue(CF_CONTENT, C_HDFS_FILE_PATH)),
				result.getValueAsByteBuffer(CF_CONTENT, C_FILE_CONTENT));
	};
}
