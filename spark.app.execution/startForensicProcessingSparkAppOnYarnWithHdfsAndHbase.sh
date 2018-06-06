#!/bin/bash
# Trigger the execution of the ForensicAnalysis Java App!
# Use local Hadoop Pseudo-Distributed Mode (incl. YARN) and Spark!
# Author: jobusam
# Work: Masterthesis

#Hadoop global params used for "hdfs" and "spark-submit" commands
JAVA_HOME="/usr/lib/jvm/java"
HADOOP_BASE_DIR="/home/johannes/Studium/Masterthesis/work/localinstance/hadoop-3.1.0"
HADOOP_CONF_DIR="$HADOOP_BASE_DIR/etc/hadoop"

#Spark global params
SPARK_EXEC_DIR="/home/johannes/Studium/Masterthesis/work/localinstance/spark-2.3.0-bin-hadoop2.7/bin"

#-----------------------------------------
#Resource params for YARN in cluster mode! On local machine on 3GB RAM can be used for app execution!
NUM_EXECUTORS=2  # maximum number of executors

DRIVER_CORES="1"
DRIVER_MEM="1152M" # 1152 = 1536M (yarn.scheduler.minimum-allocation-mb)  - 384 MB (SPARK Executor Overhead)
                   # Spark Executor Overhead = max ( 384M, 0.1 * Executor/Driver MEM)

EXECUTOR_CORES="1"
EXECUTOR_MEM="1152M" # see calculation above
#------------------------------------------

#Application params
SPARK_APP_LOCATION="/home/johannes/git/foam-processing-spark/de.foam.processing.spark/target/processing.spark-0.0.2-SNAPSHOT.jar"
SPARK_APP_MAIN_CLASS="de.foam.processing.spark.ForensicAnalysis"

# file must be on hdfs. It's also possible to use hdfs uri hdfs://localhost:9000/data
SPARK_APP_DATA_DIR="/data"
SPARK_APP_INPUT_DIR="$SPARK_APP_DATA_DIR"
SPARK_APP_OUTPUT_DIR="$SPARK_APP_DATA_DIR-results"

# HBASE dependent jars set by spark-submit --jars option! -> This solution doesn't work!
#HBASE_JARS_DIR="local:///home/johannes/Studium/Masterthesis/work/localinstance/hbase-3.0.0-SNAPSHOT/lib"
HBASE_JARS_DIR="hdfs://localhost:9000/hbase-jars/"
HBASE_JARS="$HBASE_JARS_DIR/hbase-spark-3.0.0-SNAPSHOT.jar,$HBASE_JARS_DIR/hbase-common-3.0.0-SNAPSHOT.jar,$HBASE_JARS_DIR/hbase-server-3.0.0-SNAPSHOT.jar,$HBASE_JARS_DIR/hbase-mapreduce-3.0.0-SNAPSHOT.jar,$HBASE_JARS_DIR/hbase-client-3.0.0-SNAPSHOT.jar,$HBASE_JARS_DIR/hbase-protocol-shaded-3.0.0-SNAPSHOT.jar,$HBASE_JARS_DIR/hbase-shaded-protobuf-2.1.0.jar,$HBASE_JARS_DIR/hbase-shaded-miscellaneous-2.1.0.jar,$HBASE_JARS_DIR/hbase-shaded-netty-2.1.0.jar"

# HBASE dependent jars set by spark-submit --packages options! These are the maven coordinates for the required jars. 
# This solution works correctly! Keep in mind the dependencies must be located in local maven repo. If a depedency is missing
# than download the dependency via maven into the loca .m2 repository!
MVN_PACKAGES="org.apache.hbase:hbase-spark:3.0.0-SNAPSHOT,org.apache.hbase:hbase-common:3.0.0-SNAPSHOT,org.apache.hbase:hbase-server:3.0.0-SNAPSHOT,org.apache.hbase:hbase-mapreduce:3.0.0-SNAPSHOT,org.apache.hbase:hbase-client:3.0.0-SNAPSHOT,org.apache.hbase:hbase-protocol-shaded:3.0.0-SNAPSHOT,org.apache.hbase.thirdparty:hbase-shaded-protobuf:2.1.0,org.apache.hbase.thirdparty:hbase-shaded-miscellaneous:2.1.0,org.apache.hbase.thirdparty:hbase-shaded-netty:2.1.0"

#------------------------------------------

#Export necessary params
export HADOOP_CONF_DIR=$HADOOP_CONF_DIR


if [ ! -z "$1" ] ; then
	echo "Use Input Directory = $1"
	SPARK_APP_INPUT_DIR="$1"
fi

if [ ! -z "$2" ] ; then
	echo "Use Output Directory = $2"
	SPARK_APP_OUTPUT_DIR="$2"
fi

#delete output directory before
#read -p "Delete output directory $SPARK_APP_OUTPUT_DIR from HDFS? [Y/n]:" delconf
#if [ $delconf == 'Y' ] ; then
#	echo "Deleting output directory $SPARK_APP_OUTPUT_DIR"
#	cd $HADOOP_BASE_DIR
#	./bin/hdfs dfs -rm -r $SPARK_APP_OUTPUT_DIR
#fi


cd $SPARK_EXEC_DIR
#execute application via spark-submit. Add required hbase dependencies via --packages Option. The --jars option doesn't work in local test cluster!
./spark-submit --master yarn --deploy-mode cluster --num-executors $NUM_EXECUTORS --driver-cores $DRIVER_CORES --driver-memory $DRIVER_MEM --executor-cores $EXECUTOR_CORES --executor-memory $EXECUTOR_MEM --class $SPARK_APP_MAIN_CLASS --packages "$MVN_PACKAGES" "$SPARK_APP_LOCATION" $SPARK_APP_INPUT_DIR $SPARK_APP_OUTPUT_DIR


# Version with --jars option that doesn't work correctly! -> ClassDefNotFoundErrors though the jar containing this class is given by --jars option!
# It doesn't matter if the jars a located in local fs or in HDFS. Always ClassDefNotFoundErrors occur.
#./spark-submit --master yarn --deploy-mode cluster --num-executors $NUM_EXECUTORS --driver-cores $DRIVER_CORES --driver-memory $DRIVER_MEM --executor-cores $EXECUTOR_CORES --executor-memory $EXECUTOR_MEM --class $SPARK_APP_MAIN_CLASS --jars "$HBASE_JARS" "$SPARK_APP_LOCATION" $SPARK_APP_INPUT_DIR $SPARK_APP_OUTPUT_DIR
