# set the name node 
myNameNode <- "wasb://analytics@aapocblob.blob.core.windows.net"
myPort <- 0

# set compute context to local sequential
#rxSetComputeContext("local")

# set compute context to local parallel
#rxSetComputeContext("localpar")

# set compute context to spark
mySparkCluster <- RxSpark(consoleOutput=TRUE, nameNode=myNameNode, port=myPort)
rxSetComputeContext(mySparkCluster)

packagesfun <- function()
{
  library(zoo)
  library(timeDate)
  library(stats)
  library(graphics)
  library(forecast)
  library(forecastHybrid)
  # x<-1
  # toJSON( x, method="C" )
  
}

z <- rxExec(packagesfun)