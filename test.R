# Set the NameNode and port for the cluster
myNameNode <- "https://hdiaapocblob.blob.core.windows.net"
myPort <- 0

# Set the HDFS (WASB) location of example data
bigDataDirRoot <- "/example"

# Source for the data to load
source <- system.file("AirlineDemoSmall.csv", package="RevoScaleR")

# Directory in bigDataDirRoot to load the data into
inputDir <- file.path(bigDataDirRoot,"AirlineDemoSmall.csv") 

# Make the directory
rxHadoopMakeDir(inputDir)

# Copy the data from source to input
rxHadoopCopyFromLocal(source, inputDir)

# Define the HDFS (WASB) file system
hdfsFS <- RxHdfsFileSystem(hostName=myNameNode, 
                           port=myPort)
# Create Factors for the days of the week
colInfo <- list(DayOfWeek = list(type = "factor",
                                 levels = c("Monday", 
                                            "Tuesday", 
                                            "Wednesday", 
                                            "Thursday", 
                                            "Friday", 
                                            "Saturday", 
                                            "Sunday")))
# Define the data source
airDS <- RxTextData(file = inputDir, 
                    missingValueString = "M",
                    colInfo  = colInfo, 
                    fileSystem = hdfsFS)

# Set a local compute context
rxSetComputeContext("local")

# Run a linear regression
system.time(
  modelLocal <- rxLinMod(ArrDelay~CRSDepTime+DayOfWeek,
                         data = airDS))

# Display a summary 
summary(modelLocal) 

# -- SPARK IMPLEMENTATION --

# Define the Spark compute context 
mySparkCluster <- RxSpark(consoleOutput=TRUE) 

# Set the compute context 
rxSetComputeContext(mySparkCluster) 

# Run a linear regression 
system.time(  
  modelSpark <- rxLinMod(ArrDelay~CRSDepTime+DayOfWeek, data = airDS))

# Display a summary
summary(modelSpark)