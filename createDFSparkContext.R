# point to the default store 
myNameNode <- "wasb://forecasts@aapocblob.blob.core.windows.net"
myPort <- 0

# Location of the data 
bigDataDirRoot <- "/data"   

# set compute context to local sequential
#rxSetComputeContext("local")
# set compute context to local parallel
#rxSetComputeContext("localpar")

# set compute context to spark
mySparkCluster <- RxSpark(consoleOutput=TRUE, nameNode=myNameNode, port=myPort)
rxSetComputeContext(mySparkCluster)

# define HDFS file system
hdfsFS <- RxHdfsFileSystem(hostName=myNameNode, port=myPort)

# specify the input files in HDFS to analyze
bDatIF <-file.path(bigDataDirRoot,"bDat_00000.csv")
bDatwoNAPAIF <-file.path(bigDataDirRoot,"bDatwoNAPA_00000.csv")
pmiIF <-file.path(bigDataDirRoot,"QuarterlyPMI_01apr16.csv")

# define the data sources
bDatDS <- RxTextData(file = bDatIF, fileSystem = hdfsFS)
bDatwoNAPADS <- RxTextData(file = bDatwoNAPAIF, fileSystem = hdfsFS)
pmiDS <- RxTextData(file = pmiIF, fileSystem = hdfsFS)

# define the data frame
bDat <- rxImport(inData = foo1)
bDatwoNAPA <- rxImport(inData = foo2)
pmi <- rxImport(inData = foo3)
  
argList <- list(bDatDS,bDatwoNAPADS,pmiDS)
rxExec(foo, elemArgs = argList)
