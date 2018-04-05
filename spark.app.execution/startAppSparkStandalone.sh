#!/bin/bash
# Trigger the execution of the ForensicAnalysis Java App!
# Author: jobusam
# Work: Masterthesis

SPARK_APP_LOCATION="/home/johannes/git/foam-processing-spark/de.foam.processing.spark/target/processing.spark-0.0.1-SNAPSHOT.jar"
SPARK_APP_MAIN_CLASS="de.foam.processing.spark.ForensicAnalysis"

SPARK_APP_DATA_DIR="/home/johannes/Studium/Masterthesis/work/localinstance/apps"
SPARK_APP_INPUT_DIR="$SPARK_APP_DATA_DIR/data"
SPARK_APP_OUTPUT_DIR="$SPARK_APP_DATA_DIR/data-result"


APPS_DIR="/home/johannes/Studium/Masterthesis/work/localinstance/apps/"
EXEC_DIR="/home/johannes/Studium/Masterthesis/work/localinstance/spark-2.3.0-bin-hadoop2.7/bin"
SPARK_STANDALONE_INSTANCE_URI="spark://johannes-development:7077"

if [ ! -z "$1" ] ; then
	echo "Use Input Directory = $1"
	SPARK_APP_INPUT_DIR="$1"
fi

if [ ! -z "$2" ] ; then
	echo "Use Output Directory = $2"
	SPARK_APP_OUTPUT_DIR="$2"
fi

#delete output directory before
read -p "Delete output directory $SPARK_APP_OUTPUT_DIR ? [Y/n]:" delconf
if [ $delconf == 'Y' ] ; then
	echo "Deleting output directory $SPARK_APP_OUTPUT_DIR"
	rm -rf $SPARK_APP_OUTPUT_DIR
fi

#execute application via spark-submit
cd $EXEC_DIR
./spark-submit --master $SPARK_STANDALONE_INSTANCE_URI --class $SPARK_APP_MAIN_CLASS "$SPARK_APP_LOCATION" $SPARK_APP_INPUT_DIR $SPARK_APP_OUTPUT_DIR
