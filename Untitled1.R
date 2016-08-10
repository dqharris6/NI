myNameNode <- "wasb://analytics@aapocblob.blob.core.windows.net"
myPort <- 0
bigDataDirRoot <- "/forecasts/data"   
mySparkCluster <- RxSpark(consoleOutput=TRUE, nameNode=myNameNode, port=myPort)
rxSetComputeContext("localpar")
hdfsFS <- RxHdfsFileSystem(hostName=myNameNode, port=myPort)

testfunction <- function(bigDataDirRootFx,hdfsFSFx)
{
  pmiIF <-file.path(bigDataDirRootFx,"QuarterlyPMI_01apr16.csv")
  pmiDS <- RxTextData(file = pmiIF, fileSystem = hdfsFSFx)
  pmi <- rxImport(inData = pmiDS)
  pmi<- do.call(cbind.data.frame, pmi)
  
  pmi<-pmi[!is.na(pmi$Year),]
  names(pmi)<-c("Quarter","Year","US.PMI","CHINA.PMI","JAPAN.PMI","FRA.PMI","GER.PMI","ITA.PMI","UK.PMI","GLOBAL.PMI")
  pmi$QuarterMod<-pmi$Quarter %% 4
  pmi$QuarterMod[pmi$QuarterMod == 0]<-4
  pmi$QuarterText<-as.matrix(sapply(pmi$QuarterMod,FUN=function(x){switch(x,"Q1","Q2","Q3","Q4")}))
  pmi$REQUEST_YRQTR<-paste(pmi$Year,"-",pmi$QuarterText,sep="")
  
  getForecast <- function(x,forecastType){
    
    library(forecastHybrid)
    
    # Sort by quarter, just in case
    x<-x[order(x$REQUEST_YRQTR),]
    
    if(forecastType == "GLOBAL"){
      myArimaCovar<-x$GLOBAL.PMI
      #colnames(myArimaCovar)<-names(x)[c(3)]
    }
    if(forecastType == "AMERICAS"){
      myArimaCovar<-x$US.PMI
      #colnames(myArimaCovar)<-names(x)[c(3)]
    }
    if(forecastType == "EMEIA"){
      myArimaCovar<-cbind(x$FRA.PMI,x$GER.PMI,x$ITA.PMI,x$UK.PMI)
      colnames(myArimaCovar)<-names(x)[c(3:6)]
    }
    if(forecastType == "APAC"){
      myArimaCovar<-cbind(x$CHINA.PMI,x$JAPAN.PMI)
      colnames(myArimaCovar)<-names(x)[c(3:4)]
    }
    if(!is.element(forecastType,c("GLOBAL","AMERICAS","EMEIA","APAC"))){
      myArimaCovar<-x$GLOBAL.PMI
    }
    
    
  
    
    }
  
  return(pmi)
  
}



testout <- rxExec(testfunction,bigDataDirRoot,hdfsFS)
print(testout[[1]])