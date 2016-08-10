# point to the secondary store 
myNameNode <- "wasb://forecasts@aapocblob.blob.core.windows.net"
myPort <- 0

# Location of the data 
bigDataDirRoot <- "/data"   

# set compute context to spark
mySparkCluster <- RxSpark(consoleOutput=TRUE, nameNode=myNameNode, port=myPort)
rxSetComputeContext(mySparkCluster)

# define HDFS file system
hdfsFS <- RxHdfsFileSystem(hostName=myNameNode, port=myPort)

# perform computations
functest <- function(bigDataDirRootFx, hdfsFSFx)
{
  # specify the input files in HDFS to analyze
  bDatIF <-file.path(bigDataDirRootFx,"bDat_00000.csv")
  bDat.woNAPAIF <-file.path(bigDataDirRootFx,"bDatwoNAPA_00000.csv")
  pmiIF <-file.path(bigDataDirRootFx,"QuarterlyPMI_01apr16.csv")
  
  # define the data sources
  bDatDS <- RxTextData(file = bDatIF, fileSystem = hdfsFSFx)
  bDat.woNAPADS <- RxTextData(file = bDat.woNAPAIF, fileSystem = hdfsFSFx)
  pmiDS <- RxTextData(file = pmiIF, fileSystem = hdfsFSFx)
  
  # define the data frame
  output <- list()
  
  output[[1]] <- rxImport(inData = bDatDS)
  output[[2]] <- rxImport(inData = bDat.woNAPADS)
  output[[3]] <- rxImport(inData = pmiDS)
  
  return(output)
}

# execute computations on spark context
outArr <- rxExec(functest, bigDataDirRoot, hdfsFS)

# converts the ‘R values’ to data frames
bDat <- do.call(rbind.data.frame, outArr$rxElem1[1])
bDat.woNAPA <- do.call(rbind.data.frame, outArr$rxElem1[2])
pmi<- do.call(rbind.data.frame, outArr$rxElem1[3])