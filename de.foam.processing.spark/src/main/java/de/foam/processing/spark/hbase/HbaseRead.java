package de.foam.processing.spark.hbase;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.spark.JavaHBaseContext;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Tuple2;

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

	// table name in HBASE
	private static final String HBASE_FORENSIC_TABLE = "forensicData";

	// batch size of how many gets to retrieve in a single fetch
	private static final int BATCH_SIZE = 2;

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
		Scan scans = new Scan();
		// .withStartRow(Bytes.toBytes("row1"))
		// .withStopRow(Bytes.toBytes("row20"),true);
		scans.addFamily(Bytes.toBytes("metadata"));
		// FirstKeyFilter only retrieves the row keys, but doesn't request any column!
		// scans.setFilter(new FirstKeyOnlyFilter());
		JavaRDD<Metadata> hbaseRDD = hbaseContext
				.hbaseRDD(TableName.valueOf(HBASE_FORENSIC_TABLE), scans, new ScanResultFunction())
				.map(new MetadataResultFunction());
		return hbaseRDD;
	}

	// Function to convert a value in the JavaRDD to a HBase Get
	static class GetCFMetadataFunction implements Function<byte[], Get> {
		private static final long serialVersionUID = 1L;

		public Get call(byte[] v) throws Exception {
			// Get only metadata!
			return new Get(v).addFamily(Bytes.toBytes("metadata"));
		}
	}

	static class ScanResultFunction implements Function<Tuple2<ImmutableBytesWritable, Result>, Result> {
		private static final long serialVersionUID = 1L;

		public Result call(Tuple2<ImmutableBytesWritable, Result> result) throws Exception {
			return result._2;
		}
	}

	// This will convert the HBase Result object to what ever the user wants to put
	// in the resulting JavaRDD
	static class MetadataResultFunction implements Function<Result, Metadata> {
		private static final long serialVersionUID = 1L;

		public Metadata call(Result result) throws Exception {
			return new Metadata(Bytes.toString(result.getRow()),
					Bytes.toString(result.getValue(Bytes.toBytes("metadata"), Bytes.toBytes("relativeFilePath"))),
					Bytes.toString(result.getValue(Bytes.toBytes("metadata"), Bytes.toBytes("fileType"))),
					convertFileSize(result.getValue(Bytes.toBytes("metadata"), Bytes.toBytes("fileSize"))),
					Bytes.toString(result.getValue(Bytes.toBytes("metadata"), Bytes.toBytes("owner"))),
					Bytes.toString(result.getValue(Bytes.toBytes("metadata"), Bytes.toBytes("group"))),
					Bytes.toString(result.getValue(Bytes.toBytes("metadata"), Bytes.toBytes("permissions"))),
					Bytes.toString(result.getValue(Bytes.toBytes("metadata"), Bytes.toBytes("lastModified"))),
					Bytes.toString(result.getValue(Bytes.toBytes("metadata"), Bytes.toBytes("lastChanged"))),
					Bytes.toString(result.getValue(Bytes.toBytes("metadata"), Bytes.toBytes("lastAccessed"))),
					Bytes.toString(result.getValue(Bytes.toBytes("metadata"), Bytes.toBytes("created"))));
		}

		long convertFileSize(byte[] binaryFileSize) {
			LOGGER.error("FileSize = {}", binaryFileSize);
			if (binaryFileSize != null && binaryFileSize.length > 0) {
				return Long.valueOf(Bytes.toString(binaryFileSize));
			}
			return 0L;
		}
	}

	public static JavaRDD<byte[]> createRowCount(JavaSparkContext jsc) {
		List<byte[]> list = new ArrayList<>(5);
		list.add(Bytes.toBytes("row0"));
		list.add(Bytes.toBytes("row1"));
		list.add(Bytes.toBytes("row2"));
		list.add(Bytes.toBytes("row3"));
		list.add(Bytes.toBytes("row4"));
		// Original JavaRDD with data to iterate over
		return jsc.parallelize(list);
	}

	/**
	 * Create a {@link JavaRDD} that contains a {@link Content} object for every
	 * entry in HBASE Table "forensicData". FIXME: At the moment only the first 5
	 * rows will be requested! Change implementation and do a full table scan!
	 */
	public JavaRDD<Content> getForensicFileContent(JavaSparkContext jsc) {

		// A simple abstraction over the HBaseContext.mapPartition method.
		// It allow addition support for a user to take a JavaRDD and generates a new
		// RDD based on Gets and the results they bring back from HBase
		JavaRDD<Content> bulkGet = hbaseContext.bulkGet(TableName.valueOf(HBASE_FORENSIC_TABLE), BATCH_SIZE,
				createRowCount(jsc), new GetContentFunction(), new ContentResultFunction());
		return bulkGet;
	}

	// Function to convert a value in the JavaRDD to a HBase Get
	static class GetContentFunction implements Function<byte[], Get> {
		private static final long serialVersionUID = 1L;

		public Get call(byte[] v) throws Exception {
			// FIXME: Try to get only the relevant columns!
			return new Get(v);
		}
	}

	// This will convert the HBase Result object to what ever the user wants to put
	// in the resulting JavaRDD
	static class ContentResultFunction implements Function<Result, Content> {
		private static final long serialVersionUID = 1L;

		public Content call(Result result) throws Exception {
			if ("DATA_FILE"
					.equals(Bytes.toString(result.getValue(Bytes.toBytes("metadata"), Bytes.toBytes("fileType"))))) {
				return new Content(Bytes.toString(result.getRow()),
						Bytes.toString(result.getValue(Bytes.toBytes("metadata"), Bytes.toBytes("relativeFilePath"))),
						Bytes.toString(result.getValue(Bytes.toBytes("content"), Bytes.toBytes("hdfsFilePath"))),
						result.getValueAsByteBuffer(Bytes.toBytes("content"), Bytes.toBytes("fileContent")));
			}
			return null;
		}
	}
}
