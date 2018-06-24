#!/bin/bash
# Trigger the execution of the previously built ForensicAnalysis Java App!
# Author: jobusam
# Work: Masterthesis

SPARK_APP_LOCATION="processing/processing.spark-0.0.2-SNAPSHOT.jar"
SPARK_APP_THIRDPARTY_LOCATION="processing/processing.spark-thirdparty-0.0.2-SNAPSHOT-jar-with-dependencies.jar"
SPARK_APP_MAIN_CLASS="de.foam.processing.spark.ForensicAnalysis"

HBASE_SITE_XML_FILE="/etc/hbase/conf/hbase-site.xml"
HDFS_LARGE_FILE_DIR="/user/hdtest/ubuntuImageV.1.0/"

#execute application via spark-submit
spark-submit --master yarn --deploy-mode client --class $SPARK_APP_MAIN_CLASS --jars $SPARK_APP_THIRDPARTY_LOCATION $SPARK_APP_LOCATION $HBASE_SITE_XML_FILE $HDFS_LARGE_FILE_DIR

