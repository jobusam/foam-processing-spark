package de.foam.processing.spark;

import java.util.Objects;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.foam.processing.spark.hbase.Content;
import de.foam.processing.spark.hbase.HbaseRead;

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
		try {
			HbaseRead.getForensicMetadata(jsc).collect().stream()
					.forEach(e -> LOGGER.info("Entry = {} and relativePath = {} and file size = {}.", e.getId(),
							e.getRelativeFilePath(), e.getFileSize()));

			HbaseRead.getForensicFileContent(jsc).filter(Objects::nonNull)
					// Keep in mind the data type Content is not serializable due to the containing
					// ByteBuffer. Thats the reason why the content's data will converted in single
					// string message BEFORE the collect() method is called. Because after the
					// collect everything must be available on the driver!
					.map((Content e) -> String.format(
							"Entry = %s with relativePath = %s and hdfsPath = %s and hbase content size = %d.",
							e.getId(), e.getRelativeFilePath(), e.getHdfsFilePath(),
							e.getContent() != null ? e.getContent().capacity() : -1))
					.collect().stream().forEach(e -> LOGGER.info(e));
		} finally {
			jsc.stop();
		}
	}

}