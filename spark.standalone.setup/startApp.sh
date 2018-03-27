#!/bin/bash
# Trigger the execution of the previously built ForensicAnalysis Java App!
# Author: jobusam
# Work: Masterthesis

SPARK_APP_LOCATION="/home/johannes/git/foam-processing-spark/de.foam.processing.spark/target/processing.spark-0.0.1-SNAPSHOT.jar"
SPARK_APP_MAIN_CLASS="de.foam.processing.spark.ForensicAnalysis"
SPARK_APP_OPTION="/home/johannes/Studium/Masterthesis/work/localinstance/apps/data"

APPS_DIR="/home/johannes/Studium/Masterthesis/work/localinstance/apps/"
EXEC_DIR="/home/johannes/Studium/Masterthesis/work/localinstance/spark-2.3.0-bin-hadoop2.7/bin"
SPARK_STANDALONE_INSTANCE_URI="spark://johannes-development:7077"

if [ ! -z "$1" ] ; then
		echo "Use Spark App Options = $1"
		SPARK_APP_OPTION="$1"	
fi

#execute application via spark-submit
cd $EXEC_DIR
./spark-submit --master $SPARK_STANDALONE_INSTANCE_URI --class $SPARK_APP_MAIN_CLASS "$SPARK_APP_LOCATION" $SPARK_APP_OPTION
