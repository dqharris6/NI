# CAN RUN ON LOCAL CONTEXT, BUT NOT ON MYSPARKCONTEXT. RUNNING INTO FXN NOT FOUND FOR HYBRID FORECASTS.
# TIME TO COMPLETE: ----

# Data Flow:
#   1 - Store results of Datameer export job onto secondary blob storage
#   2 - THE FOLLOWING IN THIS CODE:
#      
#       
#       Create a view of the results, save to a .csv file
#       Copy that .csv file back into blob storage
#   3 - Create a hive table from the results in Ambari, save to secondary blob storage
#   4 - Move into tableau, pick up hive table from blob storage

# Hadoop and Spark configs
myNameNode <- "wasb://forecasts@aapocblob.blob.core.windows.net"
myPort <- 0
bigDataDirRoot <- "/data"   
mySparkCluster <- RxSpark(consoleOutput=TRUE, nameNode=myNameNode, port=myPort)
rxSetComputeContext(mySparkCluster)
hdfsFS <- RxHdfsFileSystem(hostName=myNameNode, port=myPort)

require(forecastHybrid)

runForecasts<-function(bigDataDirRootFx, hdfsFSFx){
  
  pmiIF <-file.path(bigDataDirRootFx,"QuarterlyPMI_01apr16.csv")
  pmiDS <- RxTextData(file = pmiIF, fileSystem = hdfsFSFx)
  pmi <- rxImport(inData = pmiDS)
  pmi<-pmi[!is.na(pmi$Year),]
  names(pmi)<-c("Quarter","Year","US.PMI","CHINA.PMI","JAPAN.PMI","FRA.PMI","GER.PMI","ITA.PMI","UK.PMI","GLOBAL.PMI")
  pmi$QuarterMod<-pmi$Quarter %% 4
  pmi$QuarterMod[pmi$QuarterMod == 0]<-4
  pmi$QuarterText<-as.matrix(sapply(pmi$QuarterMod,FUN=function(x){switch(x,"Q1","Q2","Q3","Q4")}))
  pmi$REQUEST_YRQTR<-paste(pmi$Year,"-",pmi$QuarterText,sep="")
  
  getForecast<-function(x,geo,napaLabel){
    
    library(forecastHybrid)

    # Sort by quarter, just in case
    x<-x[order(x$REQUEST_YRQTR),]
    
    if(geo == "GLOBAL"){
      myArimaCovar<-x$GLOBAL.PMI
      #colnames(myArimaCovar)<-names(x)[c(3)]
    }
    if(geo == "AMERICAS"){
      myArimaCovar<-x$US.PMI
      #colnames(myArimaCovar)<-names(x)[c(3)]
    }
    if(geo == "EMEIA"){
      myArimaCovar<-cbind(x$FRA.PMI,x$GER.PMI,x$ITA.PMI,x$UK.PMI)
      colnames(myArimaCovar)<-names(x)[c(3:6)]
    }
    if(geo == "APAC"){
      myArimaCovar<-cbind(x$CHINA.PMI,x$JAPAN.PMI)
      colnames(myArimaCovar)<-names(x)[c(3:4)]
    }
    
    # THIS IS WHERE IT THROWS THE ERROR
    
    myTS<-ts(data=x$BOOKED_AMOUNT,start=c(2000,1),deltat=1/4)
    modH<-try(hybridModel(myTS,a.arg=list(xreg=myArimaCovar)),silent=TRUE)
    usedCovar<-TRUE
    if(!is.hybridModel(modH)){
      modH<-hybridModel(myTS)
      usedCovar<-FALSE
    }
    if(usedCovar){
      if(is.element(geo,c("EMEIA","APAC"))){
        mod<-forecast(modH,h=2,level=c(80,90,95,99),xreg=t(matrix(myArimaCovar[nrow(myArimaCovar),],ncol(myArimaCovar),2)))
      }else{
        mod<-forecast(modH,h=2,level=c(80,90,95,99),xreg=as.matrix(rep(myArimaCovar[length(myArimaCovar)],2)))
      } 
    }else{
      mod<-forecast(modH,h=2,level=c(80,90,95,99))
    }
    
    # Assemble output
    outDat<-x[c("REQUEST_YRQTR")]
    outDat$FORECAST<-0
    outDat$BOOKED_AMOUNT<-x$BOOKED_AMOUNT
    outDat$LOWER80<-NA
    outDat$LOWER90<-NA
    outDat$LOWER95<-NA
    outDat$LOWER99<-NA
    outDat$UPPER80<-NA
    outDat$UPPER90<-NA
    outDat$UPPER95<-NA
    outDat$UPPER99<-NA
    
    # this is ugly - but it scales
    temp<-capture.output(print(mod))
    temp<-temp[c(2:length(temp))]
    futureQuarters<-matrix("",length(temp),1)
    for(i in 1:length(temp)){
      tempVec<-strsplit(temp[i]," ")
      futureQuarters[i]<-paste(tempVec[[1]][1],"-",tempVec[[1]][2],sep="")
    }
    
    forecastDat<-data.frame(REQUEST_YRQTR=futureQuarters)
    forecastDat$BOOKED_AMOUNT<-mod$mean
    forecastDat$FORECAST<-1
    forecastDat$LOWER80<-mod$lower[,1]
    forecastDat$LOWER90<-mod$lower[,2]
    forecastDat$LOWER95<-mod$lower[,3]
    forecastDat$LOWER99<-mod$lower[,4]
    forecastDat$UPPER80<-mod$upper[,1]
    forecastDat$UPPER90<-mod$upper[,2]
    forecastDat$UPPER95<-mod$upper[,3]
    forecastDat$UPPER99<-mod$upper[,4]
    
    outDat<-rbind(outDat,forecastDat)
    
    names(outDat)[c(3:11)]<-paste(names(outDat)[c(3:11)],".",geo,napaLabel,sep="")
    
    return(outDat)
  }
  
  # Forecast reference date (based on most internal clock recent quarter end - specify manually if desired)
  curDate<-as.character(Sys.Date())
  curDate.year<-substring(curDate,1,4)
  curDate.month<-substring(curDate,6,7)
  curDate.quarter<-ifelse(curDate.month <= 3,1,
                          ifelse(curDate.month <= 6,2,
                                 ifelse(curDate.month <= 9,3,4)))
  refMonthDay<-switch(curDate.quarter,"03-31","06-30","09-30","12-31")
  refDate<-paste(curDate.year,"-",refMonthDay,sep="")
  
  # perform computations

  # specify the input files in HDFS to analyze
  bDatIF <-file.path(bigDataDirRootFx,"bDat_00000.csv")
  bDat.woNAPAIF <-file.path(bigDataDirRootFx,"bDatwoNAPA_00000.csv")
  # pmiIF <-file.path(bigDataDirRootFx,"QuarterlyPMI_01apr16.csv")
  
  print("one")
  
  # define the data sources
  bDatDS <- RxTextData(file = bDatIF, fileSystem = hdfsFSFx)
  bDat.woNAPADS <- RxTextData(file = bDat.woNAPAIF, fileSystem = hdfsFSFx)
  # pmiDS <- RxTextData(file = pmiIF, fileSystem = hdfsFSFx)
  
  print("two")
  
  # define the data frame
  bDat <- rxImport(inData = bDatDS)
  bDat.woNAPA <- rxImport(inData = bDat.woNAPADS)
  # THREE <- rxImport(inData = pmiDS)

  print("three")
  
  # converts the ‘R values’ to data frames
  # bDat <- do.call(cbind.data.frame, ONE)
  # bDat.woNAPA <- do.call(cbind.data.frame, TWO)
  # pmi<- do.call(cbind.data.frame, THREE)

  print("four")
    
  # List of forecasting calls with ETL before calls here - should link to each other
  
  # Global forecast (with NAPA)
  dat<-aggregate(bDat$BOOKED_AMOUNT,by=list(REQUEST_YRQTR=bDat$REQUEST_YRQTR),FUN=sum)
  names(dat)[2]<-"BOOKED_AMOUNT"
  dat<-merge(dat,pmi[c("REQUEST_YRQTR","GLOBAL.PMI")])
  forecastResults<-getForecast(dat,"GLOBAL","")
  
  # Global forecast (without NAPA)
  dat<-aggregate(bDat.woNAPA$BOOKED_AMOUNT,by=list(REQUEST_YRQTR=bDat.woNAPA$REQUEST_YRQTR),FUN=sum)
  names(dat)[2]<-"BOOKED_AMOUNT"
  dat<-merge(dat,pmi[c("REQUEST_YRQTR","GLOBAL.PMI")])
  forecastResults<-merge(forecastResults,getForecast(dat,"GLOBAL",".noNAPA"))
  
  # Americas forecast (with NAPA)
  dat<-aggregate(bDat$BOOKED_AMOUNT[bDat$GEO_CONTINENT == "Americas"],by=list(REQUEST_YRQTR=bDat$REQUEST_YRQTR[bDat$GEO_CONTINENT == "Americas"]),FUN=sum)
  names(dat)[2]<-"BOOKED_AMOUNT"
  dat<-merge(dat,pmi[c("REQUEST_YRQTR","US.PMI")])
  forecastResults<-merge(forecastResults,getForecast(dat,"AMERICAS",""))
  
  # Americas forecast (without NAPA)
  dat<-aggregate(bDat.woNAPA$BOOKED_AMOUNT[bDat.woNAPA$GEO_CONTINENT == "Americas"],by=list(REQUEST_YRQTR=bDat.woNAPA$REQUEST_YRQTR[bDat.woNAPA$GEO_CONTINENT == "Americas"]),FUN=sum)
  names(dat)[2]<-"BOOKED_AMOUNT"
  dat<-merge(dat,pmi[c("REQUEST_YRQTR","US.PMI")])
  forecastResults<-merge(forecastResults,getForecast(dat,"AMERICAS",".noNAPA"))
  
  # EMEIA forecast (with NAPA)
  dat<-aggregate(bDat$BOOKED_AMOUNT[bDat$GEO_CONTINENT == "EMEIA"],by=list(REQUEST_YRQTR=bDat$REQUEST_YRQTR[bDat$GEO_CONTINENT == "EMEIA"]),FUN=sum)
  names(dat)[2]<-"BOOKED_AMOUNT"
  dat<-merge(dat,pmi[c("REQUEST_YRQTR","FRA.PMI","GER.PMI","ITA.PMI","UK.PMI")])
  forecastResults<-merge(forecastResults,getForecast(dat,"EMEIA",""))
  
  # EMEIA forecast (without NAPA)
  dat<-aggregate(bDat.woNAPA$BOOKED_AMOUNT[bDat.woNAPA$GEO_CONTINENT == "EMEIA"],by=list(REQUEST_YRQTR=bDat.woNAPA$REQUEST_YRQTR[bDat.woNAPA$GEO_CONTINENT == "EMEIA"]),FUN=sum)
  names(dat)[2]<-"BOOKED_AMOUNT"
  dat<-merge(dat,pmi[c("REQUEST_YRQTR","FRA.PMI","GER.PMI","ITA.PMI","UK.PMI")])
  forecastResults<-merge(forecastResults,getForecast(dat,"EMEIA",".noNAPA"))
  
  # APAC forecast (with NAPA)
  dat<-aggregate(bDat$BOOKED_AMOUNT[bDat$GEO_CONTINENT == "APAC"],by=list(REQUEST_YRQTR=bDat$REQUEST_YRQTR[bDat$GEO_CONTINENT == "APAC"]),FUN=sum)
  names(dat)[2]<-"BOOKED_AMOUNT"
  dat<-merge(dat,pmi[c("REQUEST_YRQTR","CHINA.PMI","JAPAN.PMI")])
  forecastResults<-merge(forecastResults,getForecast(dat,"APAC",""))
  
  # APAC forecast (without NAPA)
  dat<-aggregate(bDat.woNAPA$BOOKED_AMOUNT[bDat.woNAPA$GEO_CONTINENT == "APAC"],by=list(REQUEST_YRQTR=bDat.woNAPA$REQUEST_YRQTR[bDat.woNAPA$GEO_CONTINENT == "APAC"]),FUN=sum)
  names(dat)[2]<-"BOOKED_AMOUNT"
  dat<-merge(dat,pmi[c("REQUEST_YRQTR","CHINA.PMI","JAPAN.PMI")])
  forecastResults<-merge(forecastResults,getForecast(dat,"APAC",".noNAPA"))
  
  return(forecastResults)
}

results <- rxExec(runForecasts, bigDataDirRoot,hdfsFS)
# results <- rxExec(runForecasts, elemArgs = list(bDat,bDat.woNAPA,pmi))
# results <- runForecasts(bigDataDirRoot,hdfsFS)
View(results)

# write.csv(results, file="results.csv")
# rxHadoopCopyFromLocal("results.csv", dest = "adl://aapocdls.azuredatalakestore.net/dalton_test")