# Set the HDFS (WASB) location of example data
bigDataDirRoot <- "/example/data"
# create a local folder for storaging data temporarily
source <- "/tmp/AirOnTimeCSV2012"
dir.create(source)
# Download data to the tmp folder
remoteDir <- "http://packages.revolutionanalytics.com/datasets/AirOnTimeCSV2012"
download.file(file.path(remoteDir, "airOT201201.csv"), file.path(source, "airOT201201.csv"))
download.file(file.path(remoteDir, "airOT201202.csv"), file.path(source, "airOT201202.csv"))
download.file(file.path(remoteDir, "airOT201203.csv"), file.path(source, "airOT201203.csv"))
download.file(file.path(remoteDir, "airOT201204.csv"), file.path(source, "airOT201204.csv"))
download.file(file.path(remoteDir, "airOT201205.csv"), file.path(source, "airOT201205.csv"))
download.file(file.path(remoteDir, "airOT201206.csv"), file.path(source, "airOT201206.csv"))
download.file(file.path(remoteDir, "airOT201207.csv"), file.path(source, "airOT201207.csv"))
download.file(file.path(remoteDir, "airOT201208.csv"), file.path(source, "airOT201208.csv"))
download.file(file.path(remoteDir, "airOT201209.csv"), file.path(source, "airOT201209.csv"))
download.file(file.path(remoteDir, "airOT201210.csv"), file.path(source, "airOT201210.csv"))
download.file(file.path(remoteDir, "airOT201211.csv"), file.path(source, "airOT201211.csv"))
download.file(file.path(remoteDir, "airOT201212.csv"), file.path(source, "airOT201212.csv"))
# Set directory in bigDataDirRoot to load the data into
inputDir <- file.path(bigDataDirRoot,"AirOnTimeCSV2012") 
# Make the directory
rxHadoopMakeDir(inputDir)
# Copy the data from source to input
rxHadoopCopyFromLocal(source, bigDataDirRoot)

# Define the HDFS (WASB) file system
hdfsFS <- RxHdfsFileSystem()
# Create info list for the airline data
airlineColInfo <- list(
  DAY_OF_WEEK = list(type = "factor"),
  ORIGIN = list(type = "factor"),
  DEST = list(type = "factor"),
  DEP_TIME = list(type = "integer"),
  ARR_DEL15 = list(type = "logical"))

# get all the column names
varNames <- names(airlineColInfo)

# Define the text data source in hdfs
airOnTimeData <- RxTextData(inputDir, colInfo = airlineColInfo, varsToKeep = varNames, fileSystem = hdfsFS)
# Define the text data source in local system
airOnTimeDataLocal <- RxTextData(source, colInfo = airlineColInfo, varsToKeep = varNames)

# formula to use
formula = "ARR_DEL15 ~ ORIGIN + DAY_OF_WEEK + DEP_TIME + DEST"

# Set a local compute context
rxSetComputeContext("local")
# Run a logistic regression
system.time(
  modelLocal <- rxLogit(formula, data = airOnTimeDataLocal)
)
# Display a summary 
summary(modelLocal)