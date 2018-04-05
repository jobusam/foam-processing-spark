#!/bin/bash
# Trigger the execution of the ForensicAnalysis Java App!
# Author: jobusam
# Work: Masterthesis

SPARK_APP_LOCATION="processing.spark-0.0.1-SNAPSHOT.jar"
SPARK_APP_MAIN_CLASS="de.foam.processing.spark.ForensicAnalysis"

SPARK_APP_DATA_DIR="/user/jbusam"
SPARK_APP_INPUT_DIR="$SPARK_APP_DATA_DIR/testdata-centos"
SPARK_APP_OUTPUT_DIR="$SPARK_APP_DATA_DIR/testdata-centos-results"


if [ ! -z "$1" ] ; then
	echo "Use Input Directory = $1"
	SPARK_APP_INPUT_DIR="$1"
fi

if [ ! -z "$2" ] ; then
	echo "Use Output Directory = $2"
	SPARK_APP_OUTPUT_DIR="$2"
fi

#delete output directory before
read -p "Delete output directory $SPARK_APP_OUTPUT_DIR from HDFS? [Y/n]:" delconf
if [ $delconf == 'Y' ] ; then
	echo "Deleting output directory $SPARK_APP_OUTPUT_DIR"
	hdfs dfs -rm -r $SPARK_APP_OUTPUT_DIR
fi

#execute application via spark-submit
spark-submit --master yarn --deploy-mode cluster --class $SPARK_APP_MAIN_CLASS $SPARK_APP_LOCATION $SPARK_APP_INPUT_DIR $SPARK_APP_OUTPUT_DIR

