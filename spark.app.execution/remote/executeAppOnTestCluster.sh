#!/bin/bash
# Trigger the execution of the ForensicAnalysis on Remote Cluster!
# 1) Copy forensic app to remote cluster
# 2) Execute the local spark-submit script remotely
# Author: jobusam
# Work: Masterthesis

#Application params
SPARK_APP_LOCATION="/home/johannes/git/foam-processing-spark/de.foam.processing.spark/target/processing.spark-0.0.2-SNAPSHOT.jar"
SPARK_APP_MAIN_CLASS="de.foam.processing.spark.ForensicAnalysis"

#The connectionParameter script contains following parameters for remote cluster.
#USER=[user_name_on_remote_cluster]
#REMOTE_HOST=[hostname_of_remote_hadoop_cluster]
#The specific configuration must not be committed to github!
source connectionParameter.sh

#Script that will be executed remotely!
SCRIPT=startApp.sh

#Copy jar file to remote host
scp $SPARK_APP_LOCATION $USER@$REMOTE_HOST:/home/$USER/processing/

# See http://backreference.org/2011/08/10/running-local-script-remotely-with-arguments/
ssh $USER@$REMOTE_HOST 'cat | bash /dev/stdin' "$@" < "$SCRIPT"
