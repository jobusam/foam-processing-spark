#!/bin/bash
# Start Spark Standalone
# Author: jobusam
# Work: Masterthesis

EXEC_DIR="/home/johannes/Studium/Masterthesis/work/localinstance/spark-2.3.0-bin-hadoop2.7/sbin"

cd $EXEC_DIR 
./start-all.sh

echo "Started Spark Standalone instance on local node"

