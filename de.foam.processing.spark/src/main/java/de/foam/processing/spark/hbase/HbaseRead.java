package de.foam.processing.spark.hbase;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.spark.JavaHBaseContext;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class creates RDDs bases with the content persisted in HBASE.
 * 
 * @author jobusam
 *
 */
public class HbaseRead {
	private static final Logger LOGGER = LoggerFactory.getLogger(HbaseRead.class);

	private static final String HBASE_HOST = "localhost";
	private static final String HBASE_PORT = "2181";
	private static final String HBASE_FORENSIC_TABLE = "forensicData";

	// batch size of how many gets to retrieve in a single fetch
	private static final int BATCH_SIZE = 2;

	public static JavaHBaseContext createHBASEContext(JavaSparkContext jsc) {
		Configuration conf = HBaseConfiguration.create();
		// HBASE/Zookeeper instance properties on localhost!
		conf.set("hbase.zookeeper.quorum", HBASE_HOST);
		conf.set("hbase.zookeeper.property.clientPort", HBASE_PORT);
		return new JavaHBaseContext(jsc, conf);
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

	public static JavaRDD<Metadata> getForensicMetadata(JavaSparkContext jsc) {
		JavaHBaseContext hbaseContext = createHBASEContext(jsc);

		// A simple abstraction over the HBaseContext.mapPartition method.
		// It allow addition support for a user to take a JavaRDD and generates a new
		// RDD based on Gets and the results they bring back from HBase
		JavaRDD<Metadata> bulkGet = hbaseContext.bulkGet(TableName.valueOf(HBASE_FORENSIC_TABLE), BATCH_SIZE,
				createRowCount(jsc), new GetCFMetadataFunction(), new MetadataResultFunction());
		return bulkGet;
	}

	// Function to convert a value in the JavaRDD to a HBase Get
	static class GetCFMetadataFunction implements Function<byte[], Get> {
		private static final long serialVersionUID = 1L;

		public Get call(byte[] v) throws Exception {
			// Get only metadata!
			return new Get(v).addFamily(Bytes.toBytes("metadata"));
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

	public static JavaRDD<Content> getForensicFileContent(JavaSparkContext jsc) {
		JavaHBaseContext hbaseContext = createHBASEContext(jsc);

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
