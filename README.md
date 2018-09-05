# foam-processing-spark
This project contains forensic applications for analysis with Apache Spark. 

## Current functionality
Following functionality is implemented:
* Read data from HBASE and HDFS.
  * Use project [foam-data-import](https://github.com/jobusam/foam-data-import) to import data into Hadoop Cluster.
* Calculate file hashes and persist the result in HBASE.
* Find duplicate files and print results into log file.
* Detect file media types (like JPEG-Image, MPEG-Video, WORD-Document,...) based on Apache Tika and persist results in HBASE.

## Future functionality
* Extract strings from file content and create an index for keyword search
* Implement a solution to display file duplicates and search for specific media types

## Build Forensic App
Checkout the Maven Project written in Java ([de.foam.processing.spark](de.foam.processing.spark)).

Install Maven and Java JDK 1.8.0 (optional)
```
# on Fedora 27
sudo dnf install maven java-1.8.0-openjdk 
```

### Build for Hortonworks Dataplatform HDP
Build the app for Hortonworks HDP 2.6.3 (Hadoop + Spark + HBASE).  
Build it with Maven and create a fat JAR file (required bundles included):
```
cd de.foam.processing.spark
mvn clean package -P buildForHDP2.6.3
```
Resulting JAR-File: target/processing.spark-0.0.2-SNAPSHOT-jar-with-dependencies.jar

### Build for Standalone Pseudo-distributed Cluster
For testing issues and SW development it's possible to execute
the app on Hadoop Standalone (HDFS + YARN) and HBASE Standalone via spark-submit command.  
Local Standalone configuration: 
* Hadoop 3.1.0
* Spark 2.3.0 (for Hadoop 2.7.0 and later)
* HBASE 3.0.0 (latest state from repository)

For accessing HBASE data in Spark the hbase-spark connector is used.  
See project [hbase-spark](https://github.com/apache/hbase/tree/master/hbase-spark).

Following steps are required to build the hbase-spark connector and HBASE in the latest version from source code:

```
# 1) clone hbase git repository
git clone https://github.com/apache/hbase.git

# 2) OPTIONAL: clear  local maven repository
rm -r ~/.m2/repository

# 3) Build HBASE and install maven artifacts into local repository
# -- build HBASE against Hadoop version 3.1.0
mvn clean install assembly:single -DskipTests=true -Dhadoop.profile=3.0 -Dhadoop-three.version=3.1.0
# The HBASE single tarball is located ./hbase-assembly/target directory.
# The tarball can be used to start an HBASE Standalone instance 
```
Now build the app against the latest HBASE Maven artifacts. 
Sometimes i run into trouble when maven wants to download some unavailable artifacts from apache snapshot repository.
Therefore it could help to comment out the apache plugin repository in the hbase root pom file (hbase/pom.xml).
```
cd de.foam.processing.spark
mvn clean package 
```
Resulting JAR-File: target/processing.spark-0.0.2-SNAPSHOT-jar-with-dependencies.jar

# Execute the Forensic App

## On Hadoop Test Cluster
The app itself can be executed on Hadoop Test Cluster via spark-submit command.
It was tested on Hortonworks HDP 2.6.3. Due to building app as fat jar there are no additional dependencies
that must be added in spark-submit command.    
See folder [spark.app.execution/remote](spark.app.execution/remote).   
The file [startApp.sh](spark.app.execution/remote/startApp.sh) contains the spark-submit command with required
parameters for execution.      
The file [executeAppOnTestCluster.sh](spark.app.execution/remote/executeAppOnTestCluster.sh) contains a shell script
than can be executed on local development pc and automatically connects to the defined test cluster, uploads the fat jar
and executes them. 

## On Standalone Pseudo-distributed Cluster
It's also possible to execute the app on a standalone pseudo-distributed cluster.
You need to download Apache Hadoop 3.1.0 and Apache Spark 2.3.0 (for Hadoop 2.7.0 and later).   
Additionally HBASE 3.0.0-SNAPSHOT must be build from source code (see [Build for Standalone Pseudo-distributed Cluster](#build-for-standalone-pseudo-distributed-cluster))

To configure Apache Hadoop and HBASE see [foam-storage-hadoop](https://github.com/jobusam/foam-storage-hadoop) project.   
The Hadoop configuration is located in [hadoop.standalone.configuration](https://github.com/jobusam/foam-storage-hadoop/tree/master/hadoop.standalone.configuration).   
The HBASE configuration is located in [hbase.standalone.configuration](https://github.com/jobusam/foam-storage-hadoop/tree/master/hbase.standalone.configuration).   

Start standalone pseudo-distributed cluster:
1. Configure passwordless access on localhost with [0.1_configurePasswordlessSSHConnect.sh](https://github.com/jobusam/foam-storage-hadoop/blob/master/hadoop.standalone.setup/0.1_configurePasswordlessSSHConnect.sh).
2. Start SSH daemon with [1.0_startSSHService.sh](https://github.com/jobusam/foam-storage-hadoop/blob/master/hadoop.standalone.setup/1.0_startSSHService.sh).
3. Start Hadoop standalone instance with [2.0_startHadoopStandalone.sh](https://github.com/jobusam/foam-storage-hadoop/blob/master/hadoop.standalone.setup/2.0_startHadoopStandalone.sh).
4. Start HBASE standalone instance with [1.0_startHbaseStandalone.sh](https://github.com/jobusam/foam-storage-hadoop/blob/master/hbase.standalone.setup/1.0_startHbaseStandalone.sh).
5. Import Data into Hadoop Cluster use project [foam-data-import](https://github.com/jobusam/foam-data-import).
6. Execute this App on standalone pseudo-distributed cluster:   
Execute [startForensicProcessingSparkAppOnYarnWithHdfsAndHbase.sh](spark.app.execution/startForensicProcessingSparkAppOnYarnWithHdfsAndHbase.sh).
7. Stop Hadoop standalone [3.0_stopHadoopStandalone.sh](https://github.com/jobusam/foam-storage-hadoop/blob/master/hadoop.standalone.setup/3.0_stopHadoopStandalone.sh).  
8. Stop HBASE standalone [2.0_stopHbaseStandalone.sh](https://github.com/jobusam/foam-storage-hadoop/blob/master/hbase.standalone.setup/2.0_stopHbaseStandalone.sh).
9. Remove passwordless access with [4.0_removePasswordlessSSHConnect.sh](https://github.com/jobusam/foam-storage-hadoop/blob/master/hadoop.standalone.setup/4.0_removePasswordlessSSHConnect.sh).


## Other
Building the App was tested on:
* Fedora 27 
  * openjdk 1.8.0_171 
  * maven 3.5.2.
  
App execution was tested on:
* Local PC
  * Hadoop 3.1.0
  * Spark 2.3.0 (for Hadoop 2.7.0 and later)
  * HBASE 3.0.0-SNAPSHOT (latest state from repository)
* Hadoop Test Cluster with Hortonworks HDP 2.6.3

Feel free to give me feedback if something doesn't work with your setup.
