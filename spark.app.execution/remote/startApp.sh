#!/bin/bash
# Trigger the execution of the previously built ForensicAnalysis Java App!
# Author: jobusam
# Work: Masterthesis

SPARK_APP_LOCATION="processing/processing.spark-0.0.2-SNAPSHOT-jar-with-dependencies.jar"
SPARK_APP_MAIN_CLASS="de.foam.processing.spark.ForensicAnalysis"

HBASE_SITE_XML_FILE="/etc/hbase/conf/hbase-site.xml"

#execute application via spark-submit
spark-submit --master yarn --deploy-mode client --class $SPARK_APP_MAIN_CLASS $SPARK_APP_LOCATION $HBASE_SITE_XML_FILE
