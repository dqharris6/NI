# point to the ADL store 
myNameNode <- "adl://aapocdls.azuredatalakestore.net"
myPort <- 0

# Location of the data  
bigDataDirRoot <- "/dalton_test"  

# set compute context to spark
#mySparkCluster <- RxSpark(consoleOutput=TRUE)
#rxSetComputeContext(mySparkCluster)

rxSetComputeContext("local")

# define HDFS file system
hdfsFS <- RxHdfsFileSystem(hostName=myNameNode, port=myPort)

# specify the input file in HDFS to analyze
inputFile <-file.path(bigDataDirRoot,"AirlineDemoSmall.csv")

# create Factors for days of the week
colInfo <- list(DayOfWeek = list(type = "factor",
                                 levels = c("Monday", "Tuesday", "Wednesday", "Thursday",
                                            "Friday", "Saturday", "Sunday")))

# define the data source 
airDS <- RxTextData(file = inputFile, missingValueString = "M",
                    colInfo  = colInfo, fileSystem = hdfsFS)

testing <- rxImport(airDS)

# Run a linear regression
# model <- rxLinMod(ArrDelay~CRSDepTime+DayOfWeek, data = airDS)
# summary(model)