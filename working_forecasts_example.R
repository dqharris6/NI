# Add STRIPE
# Add Tableau data integration

require(forecastHybrid)

# HDFS configs
myNameNode <- "wasb://analytics@aapocblob.blob.core.windows.net"
myPort <- 0
bigDataDirRoot <- "/forecasts/data"   
mySparkCluster <- RxSpark(consoleOutput=TRUE, nameNode=myNameNode, port=myPort)
rxSetComputeContext("localpar")
hdfsFS <- RxHdfsFileSystem(hostName=myNameNode, port=myPort)

# create pmi data frame
pmiDS <- RxTextData(file.path(bigDataDirRoot,"QuarterlyPMI_01apr16.csv"), fileSystem = hdfsFS)
pmi <- rxImport(inData = pmiDS)
pmi <- pmi[!is.na(pmi$Year),]
names(pmi)<-c("Quarter","Year","US.PMI","CHINA.PMI","JAPAN.PMI","FRA.PMI","GER.PMI","ITA.PMI","UK.PMI","GLOBAL.PMI")
pmi$QuarterMod<-pmi$Quarter %% 4
pmi$QuarterMod[pmi$QuarterMod == 0]<-4
pmi$QuarterText<-as.matrix(sapply(pmi$QuarterMod,FUN=function(x){switch(x,"Q1","Q2","Q3","Q4")}))
pmi$REQUEST_YRQTR<-paste(pmi$Year,"-",pmi$QuarterText,sep="")

# create bDat data frame
bDatDS <- RxTextData(file.path(bigDataDirRoot,"bDatNew_00000.csv"), fileSystem = hdfsFS)
bDat <- rxImport(inData = bDatDS)

runForecasts<-function(pmi, bDat.woNAPA){
  
  getForecast<-function(x,forecastType){
    
    # library(ggplot2)
    # library(forecast)
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
    
    myTS<-ts(data=x$BOOKED_AMOUNT,start=c(2000,1),deltat=1/4)
    modH<-try(hybridModel(myTS,a.arg=list(xreg=myArimaCovar)),silent=TRUE)
    usedCovar<-TRUE
    if(!is.hybridModel(modH)){
      modH<-hybridModel(myTS)
      usedCovar<-FALSE
    }
    if(usedCovar){
      if(is.element(forecastType,c("EMEIA","APAC"))){
        mod<-forecast(modH,h=2,level=c(80,85,90,95),xreg=t(matrix(myArimaCovar[nrow(myArimaCovar),],ncol(myArimaCovar),2)))
      }else{
        mod<-forecast(modH,h=2,level=c(80,85,90,95),xreg=as.matrix(rep(myArimaCovar[length(myArimaCovar)],2)))
      }
    }else{
      mod<-forecast(modH,h=2,level=c(80,85,90,95))
    }
    
    # Assemble output
    outDat<-x[c("REQUEST_YRQTR")]
    outDat$FORECAST<-0
    outDat$BOOKED_AMOUNT<-x$BOOKED_AMOUNT
    outDat$LOWER80<-NA
    outDat$LOWER85<-NA
    outDat$LOWER90<-NA
    outDat$LOWER95<-NA
    outDat$UPPER80<-NA
    outDat$UPPER85<-NA
    outDat$UPPER90<-NA
    outDat$UPPER95<-NA
    
    # forecastDat<-data.frame(REQUEST_YRQTR=futureQuarters) # dummy values for REQQTr
    forecastDat<-data.frame(REQUEST_YRQTR=c("fc1","fc2"), stringsAsFactors = FALSE) # dummy values for REQQTr
    forecastDat$FORECAST<-1
    forecastDat$BOOKED_AMOUNT<-mod$mean
    forecastDat$LOWER80<-mod$lower[,1]
    forecastDat$LOWER85<-mod$lower[,2]
    forecastDat$LOWER90<-mod$lower[,3]
    forecastDat$LOWER95<-mod$lower[,4]
    forecastDat$UPPER80<-mod$upper[,1]
    forecastDat$UPPER85<-mod$upper[,2]
    forecastDat$UPPER90<-mod$upper[,3]
    forecastDat$UPPER95<-mod$upper[,4]
    
    # outDat<-rbind(outDat,forecastDat)
    
    outDat <- forecastDat
    
    names(outDat)[c(3:11)]<-paste(names(outDat)[c(3:11)],".",forecastType,sep="")
    
    return(outDat)
    # return(mod)
  }
  
  aggEx <- function(forecastType, pmi, bDat.woNAPA)
  {
    if(forecastType == "AMERICAS"){
      geoContinent = "Americas"
    } else {
      geoContinent = forecastType;
    }
    
    if(forecastType == "GLOBAL"){
      dat <- aggregate(bDat.woNAPA$BOOKED_AMOUNT,by=list(REQUEST_YRQTR=bDat.woNAPA$REQUEST_YRQTR),FUN=sum)
      columnList <- c("REQUEST_YRQTR","GLOBAL.PMI")
    } else {
      dat<-aggregate(bDat.woNAPA$BOOKED_AMOUNT[bDat.woNAPA$GEO_CONTINENT == geoContinent],by=list(REQUEST_YRQTR=bDat.woNAPA$REQUEST_YRQTR[bDat.woNAPA$GEO_CONTINENT == geoContinent]),FUN=sum)
      if(forecastType == "AMERICAS"){
        columnList <- c("REQUEST_YRQTR","US.PMI")
      } else if(forecastType == "EMEIA"){
        columnList <- c("REQUEST_YRQTR","FRA.PMI","GER.PMI","ITA.PMI","UK.PMI")
      } else if(forecastType == "APAC"){
        columnList <- c("REQUEST_YRQTR","CHINA.PMI","JAPAN.PMI")
      }
    }
    names(dat)[2] <-"BOOKED_AMOUNT"
    dat <- merge(dat,pmi[c(columnList)])
    forecastResults <- getForecast(dat, forecastType)
    return(forecastResults)
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
  
  # List of forecasting calls with ETL before calls here - should link to each other
  
  # Global forecast (without NAPA)
  
  dat <- rxExec(aggEx, pmi=pmi, bDat.woNAPA=bDat.woNAPA, elemArgs=list(forecastType="GLOBAL", forecastType="AMERICAS", forecastType="EMEIA", forecastType="APAC"))
  
  # We want to rbind() the original data (with NULL columns) to each list element, and then cbind() the list elements
  newDat<-NULL
  for (i in 1:length(dat)){
    formatDat<-data.frame(REQUEST_YRQTR=sort(unique(bDat.woNAPA$REQUEST_YRQTR)),stringsAsFactors = FALSE)
    formatDat$FORECAST<-0
    
    
    temp<-switch(i,aggregate(bDat.woNAPA$BOOKED_AMOUNT,by=list(REQUEST_YRQTR=bDat.woNAPA$REQUEST_YRQTR),FUN=sum),
                 aggregate(bDat.woNAPA$BOOKED_AMOUNT[bDat.woNAPA$GEO_CONTINENT == "Americas"],by=list(REQUEST_YRQTR=bDat.woNAPA$REQUEST_YRQTR[bDat.woNAPA$GEO_CONTINENT == "Americas"]),FUN=sum),
                 aggregate(bDat.woNAPA$BOOKED_AMOUNT[bDat.woNAPA$GEO_CONTINENT == "EMEIA"],by=list(REQUEST_YRQTR=bDat.woNAPA$REQUEST_YRQTR[bDat.woNAPA$GEO_CONTINENT == "EMEIA"]),FUN=sum),
                 aggregate(bDat.woNAPA$BOOKED_AMOUNT[bDat.woNAPA$GEO_CONTINENT == "APAC"],by=list(REQUEST_YRQTR=bDat.woNAPA$REQUEST_YRQTR[bDat.woNAPA$GEO_CONTINENT == "APAC"]),FUN=sum))
    names(temp)[2]<-switch(i,"BOOKED_AMOUNT.GLOBAL","BOOKED_AMOUNT.AMERICAS","BOOKED_AMOUNT.EMEIA","BOOKED_AMOUNT.APAC")
    
    formatDat<-merge(formatDat,temp,all.x=TRUE)
    
    formatDat$LOWER80<-NA
    formatDat$LOWER85<-NA
    formatDat$LOWER90<-NA
    formatDat$LOWER95<-NA
    formatDat$UPPER80<-NA
    formatDat$UPPER85<-NA
    formatDat$UPPER90<-NA
    formatDat$UPPER95<-NA
    
    names(formatDat)[c(4:11)]<-paste(names(formatDat)[c(4:11)],switch(i,".GLOBAL",".AMERICAS",".EMEIA",".APAC"),sep="")
    
    print(names(dat[[i]]))
    
    dat[[i]]<-rbind(formatDat,dat[[i]])
    ifelse(is.null(newDat),newDat<-dat[[i]],newDat<-cbind(newDat,dat[[i]]))
  }
  
  return(newDat)
}

results <- runForecasts(pmi, bDat)

addQTR <- function(inQTR){
  if(inQTR == "Q1")
    return("Q2")
  else if(inQTR == "Q2")
    return("Q3")
  else if(inQTR == "Q3")
    return("Q4")
  else if(inQTR == "Q4")
    return("Q1")
  else
    return("NA")
}

# Based on the length of the results column (assuming start at 2000-Q1), add names to the last 2
results$REQUEST_YRQTR[length(results$REQUEST_YRQTR)-1] <- paste("20",floor((length(results$REQUEST_YRQTR)-2)/4),"-",addQTR(substr(results$REQUEST_YRQTR[length(results$REQUEST_YRQTR)-2],6,7)),sep="")
results$REQUEST_YRQTR[length(results$REQUEST_YRQTR)] <- paste("20",floor((length(results$REQUEST_YRQTR)-1)/4),"-",addQTR(substr(results$REQUEST_YRQTR[length(results$REQUEST_YRQTR)-1],6,7)),sep="")

print(results)

View(results)
# write.csv(results, file="results.csv")
# rxHadoopCopyFromLocal("results.csv", dest = "wasb://analytics@aapocblob.blob.core.windows.net/forecasts/data/")
