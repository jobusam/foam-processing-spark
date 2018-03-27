#!/bin/bash
# Stop Spark standalone instance
# Author: jobusam
# Work: Masterthesis

EXEC_DIR="/home/johannes/Studium/Masterthesis/work/localinstance/spark-2.3.0-bin-hadoop2.7"

cd $EXEC_DIR/sbin 
./stop-all.sh

echo "Spark Standalone instance on local node is stopped!"

echo "Remove Spark Log and Work directory -> Cleanup"
rm -r $EXEC_DIR/logs/*
rm -r $EXEC_DIR/work/*

