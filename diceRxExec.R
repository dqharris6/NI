playDice <- function()
{
  require(timeDate)
  require(xlsx)
  
  result <- NULL
  point <- NULL
  count <- 1
  while (is.null(result))
  {
    roll <- sum(sample(6, 2, replace=TRUE))
    
    if (is.null(point))
    {
      point <- roll
    }
    if (count == 1 && (roll == 7 || roll == 11))
    { 
      result <- "Win"
    }
    else if (count == 1 && (roll == 2 || roll == 3 || roll == 12)) 
    {
      result <- "Loss"
    } 
    else if (count > 1 && roll == 7 )
    {
      result <- "Loss"
    } 
    else if (count > 1 && point == roll)
    {
      result <- "Win"
    } 
    else
    {
      count <- count + 1
    }
  }
  result
}
   

# set the name node 
# myNameNode <- "default"
myNameNode <- "wasb://analytics@aapocblob.blob.core.windows.net"
myPort <- 0
# set compute context to local sequential
#rxSetComputeContext("local")
# set compute context to local parallel
#rxSetComputeContext("localpar")
# set compute context to spark
mySparkCluster <- RxSpark(consoleOutput=TRUE, nameNode=myNameNode, port=myPort)
rxSetComputeContext(mySparkCluster)
z <- rxExec(playDice, timesToRun=32000, taskChunkSize=2000)
table(unlist(z))      