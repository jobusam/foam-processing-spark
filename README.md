# foam-processing-spark
This project contains forensic applications for analysis with apache spark. 

## Current functionality
Following functionality is implemented:
* Find duplicated files in a given directory based on file hashes

Functionality will be improved in future ;).

## Build Forensic App
Checkout the Maven Project written java ([de.foam.processing.spark](de.foam.processing.spark)).

Install Maven and Java JDK 1.8.0 (optional)
```
# on Fedora 27
sudo dnf install maven java-1.8.0-openjdk 
```

Build it with Maven 
```
cd de.foam.processing.spark
mvn clean verify
```

## Run the Forensic App
You can run the app with a Spark Standalone instance.   
For setup please refer to [Spark Standalone Documentation](https://spark.apache.org/docs/latest/spark-standalone.html).

Additionally the folder [spark.standalone.setup](spark.standalone.setup) contains setup scripts.  
But keep in mind you have to adapt the installation paths in every scripts. I will fix that in future...

1. Configure passwordless access on localhost with [0_configurePasswordlessSSHConnect.sh](spark.standalone.setup/0_configurePasswordlessSSHConnect.sh)
2. Start SSH daemon with [1_startSSHService.sh](spark.standalone.setup/1_startSSHService.sh)
3. Start Spark Standalone Instance with [2_startSparkStandalone.sh](spark.standalone.setup/2_startSparkStandalone.sh)
4. Run the Forensic App with [startApp.sh](spark.standalone.setup/startApp.sh)  
You have to add a data directory as script parameter. This data directory will be analysed with the Forensic App.
5. Stop Spark Standalone and remove logs [3_stopSparkStandalone.sh](spark.standalone.setup/3_stopSparkStandalone.sh)  
6. Remove passwordless access with [4_removePasswordlessSSHConnect.sh](spark.standalone.setup/4_removePasswordlessSSHConnect.sh)


## Other
Tested on Fedora 27. 

Feel free to give me feedback if something doesn't work with your setup.
