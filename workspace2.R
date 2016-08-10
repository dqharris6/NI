# HDFS configs
myNameNode <- "wasb://analytics@aapocblob.blob.core.windows.net"
myPort <- 0
mySparkCluster <- RxSpark(consoleOutput=TRUE, nameNode=myNameNode, port=myPort)
rxSetComputeContext(mySparkCluster)
hdfsFS <- RxHdfsFileSystem(hostName=myNameNode, port=myPort)

# Creating filesystem
mortDefaultCsvDir <- file.path(myNameNode,"visits10AUG_good") # Where your DM export job goes
mortDefaultCsv <- RxTextData(mortDefaultCsvDir, fileSystem = hdfsFS)
mortDefaultCompXdfDir <- file.path(myNameNode,"visits10AUGxdf_good") # Have to create this folder before you can run
mortDefaultCompXdf <- RxXdfData(mortDefaultCompXdfDir, createCompositeSet=TRUE, fileSystem = hdfsFS)

# Copy files from .csv directory to new .xdf directory
rxImport(inData=mortDefaultCsv, outFile=mortDefaultCompXdf, overwrite = TRUE)

# Run a summary on the NumViewsPerVisit column with summary stats
rxSummary(formula = ~NumViewsPerVisit, data = mortDefaultCompXdf, summaryStats = c("Mean", "StdDev", "Min", "Max", "ValidObs", "MissingObs", "Sum"))

# rxDForest(formula = ~numViewsPerVisit, data = mortDefaultCompXdf)