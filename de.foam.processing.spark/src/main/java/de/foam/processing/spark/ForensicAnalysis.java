package de.foam.processing.spark;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.spark.JavaHBaseContext;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.foam.processing.spark.datamodel.Entry;
import de.foam.processing.spark.datamodel.Metadata;

/**
 * This example reads the first 5 rows of the HBASE table "forensicData" and
 * prints some informations! This is a simple example for getting records saved
 * in HBase via bulkGet function. <br>
 * <br>
 * How to build hbase-spark maven dependency: <br>
 * 1) clone git repo from hbase <a href=
 * "https://github.com/apache/hbase">https://github.com/apache/hbase</a> <br>
 * ( $ git clone https://github.com/apache/hbase.git ) <br>
 * 2) install maven (tested with version 3.5.2) <br>
 * 3) clear .m2 repository ( $ rm -r ~/.m2/repository <br>
 * 4) run $ mvn clean install assembly:single <br>
 * -DskipTests=true -Dhadoop.profile=3.0 -Dhadoop-three.version=3.1.0 <br>
 * in git hbase repository base folder <br>
 * <br>
 * 5) The HBASE single Tarball is located in ./hbase-assembly/target directory
 * <br>
 * 6) Add HBASE dependency in version 3.0.0-SNAPSHOT as dependency <br>
 * 7) Check example JavaHBaseBulkGetExample.java. <br>
 * 
 * @author jobusam
 * 
 * @see <a href=
 *      "https://github.com/apache/hbase/blob/master/hbase-spark/src/main/java/org/apache/hadoop/hbase/spark/example/hbasecontext/JavaHBaseBulkGetExample.java">JavaHBaseBulkGetExample.java</a>
 * 
 */
final public class ForensicAnalysis {
	private static final Logger LOGGER = LoggerFactory.getLogger(ForensicAnalysis.class);

	public static void main(String[] args) {
		SparkConf sparkConf = new SparkConf().setAppName("ForensicAnalysis");
		JavaSparkContext jsc = new JavaSparkContext(sparkConf);

		Configuration conf = HBaseConfiguration.create();
		// HBASE/Zookeeper instance properties on localhost!
		conf.set("hbase.zookeeper.quorum", "localhost");
		conf.set("hbase.zookeeper.property.clientPort", "2181");
		JavaHBaseContext hbaseContext = new JavaHBaseContext(jsc, conf);
		try {
			List<byte[]> list = new ArrayList<>(5);
			list.add(Bytes.toBytes("row0"));
			list.add(Bytes.toBytes("row1"));
			list.add(Bytes.toBytes("row2"));
			list.add(Bytes.toBytes("row3"));
			list.add(Bytes.toBytes("row4"));
			// Original JavaRDD with data to iterate over
			JavaRDD<byte[]> rdd = jsc.parallelize(list);

			// batch size of how many gets to retrieve in a single fetch
			int batchSize = 2;
			// A simple abstraction over the HBaseContext.mapPartition method.
			// It allow addition support for a user to take a JavaRDD and generates a new
			// RDD based on Gets and the results they bring back from HBase
			JavaRDD<Entry> bulkGet = hbaseContext.bulkGet(TableName.valueOf("forensicData"), batchSize, rdd,
					new GetFunction(), new MetadataFunction());
			// print result
			bulkGet.collect().stream().forEach(e -> LOGGER.info("Entry = {} and relativePath = {} and file size = {}.",
					e.getRowID(), e.getMetadata().getRelativeFilePath(), e.getMetadata().getFileSize()));
		} finally {
			jsc.stop();
		}
	}

	// Function to convert a value in the JavaRDD to a HBase Get
	public static class GetFunction implements Function<byte[], Get> {
		private static final long serialVersionUID = 1L;

		public Get call(byte[] v) throws Exception {
			// Get only metadata!
			return new Get(v).addFamily(Bytes.toBytes("metadata"));
		}
	}

	// This will convert the HBase Result object to what ever the user wants to put
	// in the resulting JavaRDD
	public static class MetadataFunction implements Function<Result, Entry> {
		private static final long serialVersionUID = 1L;

		public Entry call(Result result) throws Exception {
			Metadata metadata = new Metadata(
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
			return new Entry(Bytes.toString(result.getRow()), metadata);
		}

		long convertFileSize(byte[] binaryFileSize) {
			LOGGER.error("FileSize = {}", binaryFileSize);
			if (binaryFileSize != null && binaryFileSize.length > 0) {
				return Long.valueOf(Bytes.toString(binaryFileSize));
			}
			return 0L;
		}
	}
}