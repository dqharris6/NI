require(forecastHybrid)

# HDFS configs
myNameNode <- "wasb://analytics@aapocblob.blob.core.windows.net"
myPort <- 0
bigDataDirRoot <- "/forecasts/data"   
mySparkCluster <- RxSpark(consoleOutput=TRUE, nameNode=myNameNode, port=myPort)
rxSetComputeContext(mySparkCluster)
hdfsFS <- RxHdfsFileSystem(hostName=myNameNode, port=myPort)

runForecasts<-function(){
  
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
    
    nameCap<-toupper(forecastType)
    nameCap<-gsub(" ","",nameCap)
    if(nameCap == "EMBEDDEDSYSTEMS"){
      nameCap<-"EMBEDDED"
    }

    names(outDat)[c(3:11)]<-paste(names(outDat)[c(3:11)],".",nameCap,sep="")
    
    return(outDat)
    # return(mod)
  }
  
  #aggEx <- function(geoType, stripeType, pmi, bDat.woNAPA)
  aggEx <- function(forecastType, pmi, bDat.woNAPA)
  {
    # if(is.null(geoType)){
    #   forecastType<-stripeType
    if (!is.element(forecastType,c("GLOBAL","AMERICAS","EMEIA","APAC"))){
      stripeType<-forecastType
      
      #print(stripeType)
      #print(stripeNameCap)
      
      dat <- aggregate(bDat.woNAPA$BOOKED_AMOUNT[bDat.woNAPA$STRIPE == stripeType],by=list(REQUEST_YRQTR=bDat.woNAPA$REQUEST_YRQTR[bDat.woNAPA$STRIPE == stripeType]),FUN=sum)
      #dat <- aggregate(bDat.woNAPA$BOOKED_AMOUNT[bDat.woNAPA$STRIPE == forecastType],by=list(REQUEST_YRQTR=bDat.woNAPA$REQUEST_YRQTR[bDat.woNAPA$STRIPE == forecastType]),FUN=sum)
      columnList <- c("REQUEST_YRQTR","GLOBAL.PMI")
    }else{
      geoType<-forecastType
      if(geoType == "AMERICAS"){
        geoContinent = "Americas"
      } else {
        geoContinent = geoType;
      }
      
      if(geoType == "GLOBAL"){
        dat <- aggregate(bDat.woNAPA$BOOKED_AMOUNT,by=list(REQUEST_YRQTR=bDat.woNAPA$REQUEST_YRQTR),FUN=sum)
        columnList <- c("REQUEST_YRQTR","GLOBAL.PMI")
      } else {
        dat<-aggregate(bDat.woNAPA$BOOKED_AMOUNT[bDat.woNAPA$GEO_CONTINENT == geoContinent],by=list(REQUEST_YRQTR=bDat.woNAPA$REQUEST_YRQTR[bDat.woNAPA$GEO_CONTINENT == geoContinent]),FUN=sum)
        if(geoType == "AMERICAS"){
          columnList <- c("REQUEST_YRQTR","US.PMI")
        } else if(geoType == "EMEIA"){
          columnList <- c("REQUEST_YRQTR","FRA.PMI","GER.PMI","ITA.PMI","UK.PMI")
        } else if(geoType == "APAC"){
          columnList <- c("REQUEST_YRQTR","CHINA.PMI","JAPAN.PMI")
        }
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
  # geoNames<-c("GLOBAL","AMERICAS","EMEIA","APAC"," ")
  # stripeNames<-c("Academic","Core Test","Data Acquisition","Embedded Systems","Global Services","RF","Software"," ")
  # allNames<-expand.grid(stripeNames,geoNames,stringsAsFactors=FALSE)
  # allNames<-allNames[xor(allNames[,1] == " ",allNames[,2] == " "),]
  # allNames<-cbind(allNames[,2],allNames[,1])
  # 
  # forecastTypeList<-list()
  # for(i in 1:nrow(allNames)){
  #   forecastTypeList[[i]]<-list(geoType=as.character(allNames[i,1]),stripeType=as.character(allNames[i,2]))
  # }
  
  #dat <- rxExec(aggEx, pmi=pmi, bDat.woNAPA=bDat.woNAPA, elemArgs=list(geoType="GLOBAL", geoType="AMERICAS", geoType="EMEIA", geoType="APAC"))
  dat <- rxExec(aggEx, pmi=pmi, bDat.woNAPA=bDat, elemArgs=list(forecastType="GLOBAL", forecastType="AMERICAS", forecastType="EMEIA", forecastType="APAC",forecastType="Academic",forecastType="Core Test",forecastType="Data Acquisition",forecastType="Embedded Systems",forecastType="Global Services",forecastType="RF",forecastType="Software"))
  #dat <- rxExec(aggEx, pmi=pmi, bDat.woNAPA=bDat, elemArgs=forecastTypeList)
  #View(dat)
  
  # We want to rbind() the original data (with NULL columns) to each list element, and then cbind() the list elements
  newDat<-NULL
  for (i in 1:length(dat)){
    formatDat<-data.frame(REQUEST_YRQTR=sort(unique(bDat$REQUEST_YRQTR)),stringsAsFactors = FALSE)
    formatDat$FORECAST<-0
    
    temp<-switch(i,aggregate(bDat$BOOKED_AMOUNT,by=list(REQUEST_YRQTR=bDat$REQUEST_YRQTR),FUN=sum),
                  aggregate(bDat$BOOKED_AMOUNT[bDat$GEO_CONTINENT == "Americas"],by=list(REQUEST_YRQTR=bDat$REQUEST_YRQTR[bDat$GEO_CONTINENT == "Americas"]),FUN=sum),
                 aggregate(bDat$BOOKED_AMOUNT[bDat$GEO_CONTINENT == "EMEIA"],by=list(REQUEST_YRQTR=bDat$REQUEST_YRQTR[bDat$GEO_CONTINENT == "EMEIA"]),FUN=sum),
                 aggregate(bDat$BOOKED_AMOUNT[bDat$GEO_CONTINENT == "APAC"],by=list(REQUEST_YRQTR=bDat$REQUEST_YRQTR[bDat$GEO_CONTINENT == "APAC"]),FUN=sum),
                 aggregate(bDat$BOOKED_AMOUNT[bDat$STRIPE == "Academic"],by=list(REQUEST_YRQTR=bDat$REQUEST_YRQTR[bDat$STRIPE == "Academic"]),FUN=sum),
                 aggregate(bDat$BOOKED_AMOUNT[bDat$STRIPE == "Core Test"],by=list(REQUEST_YRQTR=bDat$REQUEST_YRQTR[bDat$STRIPE == "Core Test"]),FUN=sum),
                 aggregate(bDat$BOOKED_AMOUNT[bDat$STRIPE == "Data Acquisition"],by=list(REQUEST_YRQTR=bDat$REQUEST_YRQTR[bDat$STRIPE == "Data Acquisition"]),FUN=sum),
                 aggregate(bDat$BOOKED_AMOUNT[bDat$STRIPE == "Embedded Systems"],by=list(REQUEST_YRQTR=bDat$REQUEST_YRQTR[bDat$STRIPE == "Embedded Systems"]),FUN=sum),
                 aggregate(bDat$BOOKED_AMOUNT[bDat$STRIPE == "Global Services"],by=list(REQUEST_YRQTR=bDat$REQUEST_YRQTR[bDat$STRIPE == "Global Services"]),FUN=sum),
                 aggregate(bDat$BOOKED_AMOUNT[bDat$STRIPE == "RF"],by=list(REQUEST_YRQTR=bDat$REQUEST_YRQTR[bDat$STRIPE == "RF"]),FUN=sum),
                 aggregate(bDat$BOOKED_AMOUNT[bDat$STRIPE == "Software"],by=list(REQUEST_YRQTR=bDat$REQUEST_YRQTR[bDat$STRIPE == "Software"]),FUN=sum))
    names(temp)[2]<-switch(i,"BOOKED_AMOUNT.GLOBAL","BOOKED_AMOUNT.AMERICAS","BOOKED_AMOUNT.EMEIA","BOOKED_AMOUNT.APAC","BOOKED_AMOUNT.ACADEMIC","BOOKED_AMOUNT.CORETEST","BOOKED_AMOUNT.DATAACQUISITION","BOOKED_AMOUNT.EMBEDDED","BOOKED_AMOUNT.GLOBALSERVICES","BOOKED_AMOUNT.RF","BOOKED_AMOUNT.SOFTWARE")
    
    formatDat<-merge(formatDat,temp,all.x=TRUE)
    
    formatDat$LOWER80<-NA
    formatDat$LOWER85<-NA
    formatDat$LOWER90<-NA
    formatDat$LOWER95<-NA
    formatDat$UPPER80<-NA
    formatDat$UPPER85<-NA
    formatDat$UPPER90<-NA
    formatDat$UPPER95<-NA
    
    names(formatDat)[c(4:11)]<-paste(names(formatDat)[c(4:11)],switch(i,".GLOBAL",".AMERICAS",".EMEIA",".APAC",".ACADEMIC",".CORETEST",".DATAACQUISITION",".EMBEDDED",".GLOBALSERVICES",".RF",".SOFTWARE"),sep="")
    
    # print(names(dat[[i]]))
    
    dat[[i]]<-rbind(formatDat,dat[[i]])
    ifelse(is.null(newDat),newDat<-dat[[i]],newDat<-cbind(newDat,dat[[i]]))
  }
  
  # Compute YOY values
  newDat$YOYGROWTH.GLOBAL<-NA
  newDat$YOYGROWTH.AMERICAS<-NA
  newDat$YOYGROWTH.EMEIA<-NA
  newDat$YOYGROWTH.APAC<-NA
  newDat$YOYGROWTH.ACADEMIC<-NA
  newDat$YOYGROWTH.CORETEST<-NA
  newDat$YOYGROWTH.DATAACQUISITION<-NA
  newDat$YOYGROWTH.EMBEDDED<-NA
  newDat$YOYGROWTH.GLOBALSERVICES<-NA
  newDat$YOYGROWTH.RF<-NA
  newDat$YOYGROWTH.SOFTWARE<-NA
  
  for (i in c(5:nrow(newDat))){
    newDat$YOYGROWTH.GLOBAL[i]<-newDat$BOOKED_AMOUNT.GLOBAL[i]/newDat$BOOKED_AMOUNT.GLOBAL[(i-4)] - 1
    newDat$YOYGROWTH.AMERICAS[i]<-newDat$BOOKED_AMOUNT.AMERICAS[i]/newDat$BOOKED_AMOUNT.AMERICAS[(i-4)] - 1
    newDat$YOYGROWTH.EMEIA[i]<-newDat$BOOKED_AMOUNT.EMEIA[i]/newDat$BOOKED_AMOUNT.EMEIA[(i-4)] - 1
    newDat$YOYGROWTH.APAC[i]<-newDat$BOOKED_AMOUNT.APAC[i]/newDat$BOOKED_AMOUNT.APAC[(i-4)] - 1
    newDat$YOYGROWTH.ACADEMIC[i]<-newDat$BOOKED_AMOUNT.ACADEMIC[i]/newDat$BOOKED_AMOUNT.ACADEMIC[(i-4)] - 1
    newDat$YOYGROWTH.CORETEST[i]<-newDat$BOOKED_AMOUNT.CORETEST[i]/newDat$BOOKED_AMOUNT.CORETEST[(i-4)] - 1
    newDat$YOYGROWTH.DATAACQUISITION[i]<-newDat$BOOKED_AMOUNT.DATAACQUISITION[i]/newDat$BOOKED_AMOUNT.DATAACQUISITION[(i-4)] - 1
    newDat$YOYGROWTH.EMBEDDED[i]<-newDat$BOOKED_AMOUNT.EMBEDDED[i]/newDat$BOOKED_AMOUNT.EMBEDDED[(i-4)] - 1
    newDat$YOYGROWTH.GLOBALSERVICES[i]<-newDat$BOOKED_AMOUNT.GLOBALSERVICES[i]/newDat$BOOKED_AMOUNT.GLOBALSERVICES[(i-4)] - 1
    newDat$YOYGROWTH.RF[i]<-newDat$BOOKED_AMOUNT.RF[i]/newDat$BOOKED_AMOUNT.RF[(i-4)] - 1
    newDat$YOYGROWTH.SOFTWARE[i]<-newDat$BOOKED_AMOUNT.SOFTWARE[i]/newDat$BOOKED_AMOUNT.SOFTWARE[(i-4)] - 1
  }
  
  return(newDat)
}

results <- runForecasts()

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

# Based on the length of the results column (assuming start at 2000-Q1), add dates to the forecasted rows
results$REQUEST_YRQTR[length(results$REQUEST_YRQTR)-1] <- paste("20",floor((length(results$REQUEST_YRQTR)-2)/4),"-",addQTR(substr(results$REQUEST_YRQTR[length(results$REQUEST_YRQTR)-2],6,7)),sep="")
results$REQUEST_YRQTR[length(results$REQUEST_YRQTR)] <- paste("20",floor((length(results$REQUEST_YRQTR)-1)/4),"-",addQTR(substr(results$REQUEST_YRQTR[length(results$REQUEST_YRQTR)-1],6,7)),sep="")

# print(results$REQUEST_YRQTR[length(results$REQUEST_YRQTR)-1])
# print(results$REQUEST_YRQTR[length(results$REQUEST_YRQTR)])

# Convert dates (for Tableau compatibility)
results$REQUEST_YRQTR<-sapply(results$REQUEST_YRQTR,FUN=function(x){
  y<-substring(x,1,4)
  quarter<-substring(x,6,7)
  quarterDayMon<-switch(quarter,Q1="-01-01",Q2="-04-01",Q3="-07-01",Q4="-10-01")
  return(paste(y,quarterDayMon,sep=""))
})

# temp<-return(results)

# print(results)

# removes the duplicate columns, keeps the first one
results <- results[, !duplicated(colnames(results))]

View(results)
write.csv(results, file="results.csv")
rxHadoopCopyFromLocal("results.csv", dest = "wasb://analytics@aapocblob.blob.core.windows.net/forecasts/data/")
