# HDFS configs
myNameNode <- "wasb://analytics@aapocblob.blob.core.windows.net"
myPort <- 0
bigDataDirRoot <- "/clickstream/data"
mySparkCluster <- RxSpark(consoleOutput=TRUE, nameNode=myNameNode, port=myPort)
rxSetComputeContext("local")
hdfsFS <- RxHdfsFileSystem(hostName=myNameNode, port=myPort)

# Define the text data source in hdfs
visitsDS <- RxTextData(file.path(bigDataDirRoot,"visitsV2_cloudTest_visits_1470084191247.csv"), fileSystem = hdfsFS, stringsAsFactors = FALSE)

# Column names of visits dataset
# abbrev_visits <- read.csv("visitsV2_cloudTest_visits_1470084191247.csv", nrows = 1000)
# visits_cols <- names(abbrev_visits)
# visits_cols

# bigVisitsDS <- visits_cols

rxOpen(visitsDS)
bigVisitsDS <- rxReadNext(visitsDS) # bring in next chunk of dataset (500,000 rows per chunk)
bigVisitsDS <- bigVisitsDS[,1:50]   # subset the first 10 columns (in the interest of time)

for(i in 1:1){
  temp <- rxReadNext(visitsDS)
  temp <- temp[,1:10]
  bigVisitsDS <- rbind(bigVisitsDS,temp)
}

rxClose(visitsDS)

# Summary of the visits dataset
# system.time(
# results_summ <- rxSummary(formula = ~firstClickInVisit+lengthOfVisit, data = visitsDS, summaryStats = "Sum")
# )

# Histogram of the visits dataset
# system.time(
#   results_hist <- rxHistogram(formula = ~visitID|NumUserAcctSt, data = visitsDS)
# )

# Linear model of the visits dataset
# system.time(
#   results_linmod <- rxLinMod(NumViewsPerVisit~lastCamp, data = visitsDS)
# )