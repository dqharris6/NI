rxOptions(fileSystem = RxHdfsFileSystem())
dataDir <- "/home/dharris/dalton_test"

computeContext <- RxSpark(consoleOutput = TRUE)
computeContextR <- RxComputeContext(computeContext)

myHadoopCluster <- RxHadoopMR()
rxSetComputeContext(myHadoopCluster)

Sys.time(
  bDat <- read.csv("bDat.txt",stringsAsFactors = FALSE,sep=",")
)

Sys.time(
  rxExec(read.csv("bDat.txt",stringsAsFactors = FALSE,sep=","))
}