# HDFS configs
myNameNode <- "wasb://analytics@aapocblob.blob.core.windows.net"
myPort <- 0
bigDataDirRoot <- "/clickstream/data"
mySparkCluster <- RxSpark(consoleOutput=TRUE, nameNode=myNameNode, port=myPort)
rxSetComputeContext("local")
hdfsFS <- RxHdfsFileSystem(hostName=myNameNode, port=myPort)

# Example 2 
# input_data_EX2 <- file.path(myNameNode,"visits_9AUG_no_num_00000")
# textdata_in_EX2 <- RxTextData(file = input_data_EX2, fileSystem = hdfsFS, rowsPerRead = 500000, missingValueString = "NA", stringsAsFactors = FALSE)
# import_in_EX2 <- rxImport(inData = textdata_in_EX2, outFile = "test_xdf_EX2.xdf", overwrite = TRUE)
# rxSummary(formula = ~NumViewsPerVisit, data = "test_xdf_EX2.xdf", summaryStats = c("Mean", "StdDev", "Min", "Max", "ValidObs", "MissingObs", "Sum"))

for (i in 0:9){
  input_data_EX2 <- paste(file.path(myNameNode,"visits_9AUG_no_num_0000"),i,sep = "")
  print(input_data_EX2)
  textdata_in_EX2 <- rxGetOption("bigDataDirRoot")
  textdata_in_EX2 <- RxTextData(file = input_data_EX2, fileSystem = hdfsFS, rowsPerRead = 500000, missingValueString = "NA", stringsAsFactors = FALSE)
  import_in_EX2 <- rxImport(inData = textdata_in_EX2, outFile = "test_xdf_EX2.xdf", append = "rows", overwrite = FALSE)
}

for (j in 10:60)
{
  input_data_EX2 <- paste(file.path(myNameNode,"visits_9AUG_no_num_000"),j,sep = "")
  print(input_data_EX2)
  textdata_in_EX2 <- RxTextData(file = input_data_EX2, fileSystem = hdfsFS, rowsPerRead = 500000, missingValueString = "NA", stringsAsFactors = FALSE)
  import_in_EX2 <- rxImport(inData = textdata_in_EX2, outFile = "test_xdf_EX2.xdf", append = "rows", overwrite = FALSE)
}

rxSummary(formula = ~NumViewsPerVisit, data = "test_xdf_EX2.xdf", summaryStats = c("Mean", "StdDev", "Min", "Max", "ValidObs", "MissingObs", "Sum"))