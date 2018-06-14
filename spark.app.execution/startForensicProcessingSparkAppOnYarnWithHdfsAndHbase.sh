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

#use client mode to see everything in console. Normally use "cluster" mode!
DEPLOY_MODE="client" 

#Application params
SPARK_APP_LOCATION="/home/johannes/git/foam-processing-spark/de.foam.processing.spark/target/processing.spark-0.0.2-SNAPSHOT.jar"
SPARK_FAT_APP_LOCATION="/home/johannes/git/foam-processing-spark/de.foam.processing.spark/target/processing.spark-0.0.2-SNAPSHOT-jar-with-dependencies.jar"
SPARK_APP_MAIN_CLASS="de.foam.processing.spark.ForensicAnalysis"

#Export necessary params
export HADOOP_CONF_DIR=$HADOOP_CONF_DIR

cd $SPARK_EXEC_DIR

./spark-submit --master yarn --deploy-mode $DEPLOY_MODE --num-executors $NUM_EXECUTORS --driver-cores $DRIVER_CORES --driver-memory $DRIVER_MEM --executor-cores $EXECUTOR_CORES --executor-memory $EXECUTOR_MEM --class $SPARK_APP_MAIN_CLASS $SPARK_FAT_APP_LOCATION


