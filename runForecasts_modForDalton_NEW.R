# Run Forecasts
# HDF: Modify these package statements to meet your requirements

# Install the required packages on the edge node, only works across the cluster if this is done first.
# system("./InstallRPackages.sh")
# Then, re-run the 'forecasting-packages-not-persisted' script action on the cluster

require(forecastHybrid)

myNameNode <- "wasb://analytics@aapocblob.blob.core.windows.net"
myPort <- 0
bigDataDirRoot <- "/forecasts/data"   
mySparkCluster <- RxSpark(consoleOutput=TRUE, nameNode=myNameNode, port=myPort)
rxSetComputeContext("local")
hdfsFS <- RxHdfsFileSystem(hostName=myNameNode, port=myPort)

bDat.woNAPA <- read.csv("bDatwoNAPA_00000.csv")

pmiIF <-file.path(bigDataDirRoot,"QuarterlyPMI_01apr16.csv")
pmiDS <- RxTextData(file = pmiIF, fileSystem = hdfsFS)
pmi <- rxImport(inData = pmiDS)
pmi<- do.call(cbind.data.frame, pmi)

# pmi <- read.csv("QuarterlyPMI_01apr16.csv")

pmi<-pmi[!is.na(pmi$Year),]
names(pmi)<-c("Quarter","Year","US.PMI","CHINA.PMI","JAPAN.PMI","FRA.PMI","GER.PMI","ITA.PMI","UK.PMI","GLOBAL.PMI")
pmi$QuarterMod<-pmi$Quarter %% 4
pmi$QuarterMod[pmi$QuarterMod == 0]<-4
pmi$QuarterText<-as.matrix(sapply(pmi$QuarterMod,FUN=function(x){switch(x,"Q1","Q2","Q3","Q4")}))
pmi$REQUEST_YRQTR<-paste(pmi$Year,"-",pmi$QuarterText,sep="")

runForecasts<-function(bDat.woNAPAFx, pmiFx){
  
  pmi <- pmiFx
  
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
    forecastDat$LOWER85<-mod$lower[,2]
    forecastDat$LOWER90<-mod$lower[,3]
    forecastDat$LOWER95<-mod$lower[,4]
    forecastDat$UPPER80<-mod$upper[,1]
    forecastDat$UPPER85<-mod$upper[,2]
    forecastDat$UPPER90<-mod$upper[,3]
    forecastDat$UPPER95<-mod$upper[,4]
    
    outDat<-rbind(outDat,forecastDat)
    
    names(outDat)[c(3:11)]<-paste(names(outDat)[c(3:11)],".",forecastType,sep="")
    
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
  
  bDat.woNAPA <- bDat.woNAPAFx
  
    # Global forecast (without NAPA)
  dat<-aggregate(bDat.woNAPA$BOOKED_AMOUNT,by=list(REQUEST_YRQTR=bDat.woNAPA$REQUEST_YRQTR),FUN=sum)
  names(dat)[2]<-"BOOKED_AMOUNT"
  dat<-merge(dat,pmi[c("REQUEST_YRQTR","GLOBAL.PMI")])
  forecastResults<-getForecast(dat,"GLOBAL")
  
  # Americas forecast (without NAPA)
  dat<-aggregate(bDat.woNAPA$BOOKED_AMOUNT[bDat.woNAPA$GEO_CONTINENT == "Americas"],by=list(REQUEST_YRQTR=bDat.woNAPA$REQUEST_YRQTR[bDat.woNAPA$GEO_CONTINENT == "Americas"]),FUN=sum)
  names(dat)[2]<-"BOOKED_AMOUNT"
  dat<-merge(dat,pmi[c("REQUEST_YRQTR","US.PMI")])
  forecastResults<-merge(forecastResults,getForecast(dat,"AMERICAS"))
  
  # EMEIA forecast (without NAPA)
  dat<-aggregate(bDat.woNAPA$BOOKED_AMOUNT[bDat.woNAPA$GEO_CONTINENT == "EMEIA"],by=list(REQUEST_YRQTR=bDat.woNAPA$REQUEST_YRQTR[bDat.woNAPA$GEO_CONTINENT == "EMEIA"]),FUN=sum)
  names(dat)[2]<-"BOOKED_AMOUNT"
  dat<-merge(dat,pmi[c("REQUEST_YRQTR","FRA.PMI","GER.PMI","ITA.PMI","UK.PMI")])
  forecastResults<-merge(forecastResults,getForecast(dat,"EMEIA"))
    
  # APAC forecast (without NAPA)
  dat<-aggregate(bDat.woNAPA$BOOKED_AMOUNT[bDat.woNAPA$GEO_CONTINENT == "APAC"],by=list(REQUEST_YRQTR=bDat.woNAPA$REQUEST_YRQTR[bDat.woNAPA$GEO_CONTINENT == "APAC"]),FUN=sum)
  names(dat)[2]<-"BOOKED_AMOUNT"
  dat<-merge(dat,pmi[c("REQUEST_YRQTR","CHINA.PMI","JAPAN.PMI")])
  forecastResults<-merge(forecastResults,getForecast(dat,"APAC"))
  
  # Stripe forecasts
  stripeNames<-sort(unique(bDat.woNAPA$STRIPE))
  stripeNames<-stripeNames[!is.element(stripeNames,c("Acquisitions - Year One Only","Horizontal Products","No Stripe Assigned"))]
  stripeNamesCap<-toupper(stripeNames)
  stripeNamesCap<-gsub(" ","",stripeNamesCap)
  stripeNamesCap[4]<-"EMBEDDED"
  
  for(i in 1:length(stripeNames)){
    dat<-aggregate(bDat.woNAPA$BOOKED_AMOUNT[bDat.woNAPA$STRIPE == stripeNames[i]],by=list(REQUEST_YRQTR=bDat.woNAPA$REQUEST_YRQTR[bDat.woNAPA$STRIPE == stripeNames[i]]),FUN=sum)
    names(dat)[2]<-"BOOKED_AMOUNT"
    if(nrow(dat) < (nrow(forecastResults)-2)){
      # Some dates are not present - compensate
      temp<-head(unique(forecastResults[c("REQUEST_YRQTR")]),nrow(forecastResults)-2)
      dat<-merge(temp,dat,all.x=TRUE)
      dat<-aggregate(dat$BOOKED_AMOUNT,by=list(REQUEST_YRQTR=dat$REQUEST_YRQTR),FUN=sum,na.rm=TRUE)
      names(dat)[2]<-"BOOKED_AMOUNT"
    }
    
    dat<-merge(dat,pmi[c("REQUEST_YRQTR","GLOBAL.PMI")])
    forecastResults<-merge(forecastResults,getForecast(dat,stripeNamesCap[i])) 
  }
  
  # Compute YOY values
  forecastResults$YOYGROWTH.GLOBAL<-NA
  forecastResults$YOYGROWTH.AMERICAS<-NA
  forecastResults$YOYGROWTH.EMEIA<-NA
  forecastResults$YOYGROWTH.APAC<-NA
  forecastResults$YOYGROWTH.ACADEMIC<-NA
  forecastResults$YOYGROWTH.CORETEST<-NA
  forecastResults$YOYGROWTH.DATAACQUISITION<-NA
  forecastResults$YOYGROWTH.EMBEDDED<-NA
  forecastResults$YOYGROWTH.GLOBALSERVICES<-NA
  forecastResults$YOYGROWTH.RF<-NA
  forecastResults$YOYGROWTH.SOFTWARE<-NA
  
  for (i in c(5:nrow(forecastResults))){
    forecastResults$YOYGROWTH.GLOBAL[i]<-forecastResults$BOOKED_AMOUNT.GLOBAL[i]/forecastResults$BOOKED_AMOUNT.GLOBAL[(i-4)] - 1
    forecastResults$YOYGROWTH.AMERICAS[i]<-forecastResults$BOOKED_AMOUNT.AMERICAS[i]/forecastResults$BOOKED_AMOUNT.AMERICAS[(i-4)] - 1
    forecastResults$YOYGROWTH.EMEIA[i]<-forecastResults$BOOKED_AMOUNT.EMEIA[i]/forecastResults$BOOKED_AMOUNT.EMEIA[(i-4)] - 1
    forecastResults$YOYGROWTH.APAC[i]<-forecastResults$BOOKED_AMOUNT.APAC[i]/forecastResults$BOOKED_AMOUNT.APAC[(i-4)] - 1
    forecastResults$YOYGROWTH.ACADEMIC[i]<-forecastResults$BOOKED_AMOUNT.ACADEMIC[i]/forecastResults$BOOKED_AMOUNT.ACADEMIC[(i-4)] - 1
    forecastResults$YOYGROWTH.CORETEST[i]<-forecastResults$BOOKED_AMOUNT.CORETEST[i]/forecastResults$BOOKED_AMOUNT.CORETEST[(i-4)] - 1
    forecastResults$YOYGROWTH.DATAACQUISITION[i]<-forecastResults$BOOKED_AMOUNT.DATAACQUISITION[i]/forecastResults$BOOKED_AMOUNT.DATAACQUISITION[(i-4)] - 1
    forecastResults$YOYGROWTH.EMBEDDED[i]<-forecastResults$BOOKED_AMOUNT.EMBEDDED[i]/forecastResults$BOOKED_AMOUNT.EMBEDDED[(i-4)] - 1
    forecastResults$YOYGROWTH.GLOBALSERVICES[i]<-forecastResults$BOOKED_AMOUNT.GLOBALSERVICES[i]/forecastResults$BOOKED_AMOUNT.GLOBALSERVICES[(i-4)] - 1
    forecastResults$YOYGROWTH.RF[i]<-forecastResults$BOOKED_AMOUNT.RF[i]/forecastResults$BOOKED_AMOUNT.RF[(i-4)] - 1
    forecastResults$YOYGROWTH.SOFTWARE[i]<-forecastResults$BOOKED_AMOUNT.SOFTWARE[i]/forecastResults$BOOKED_AMOUNT.SOFTWARE[(i-4)] - 1
  }
  
  # Convert dates (for Tableau compatibility)
  forecastResults$REQUEST_YRQTR<-sapply(forecastResults$REQUEST_YRQTR,FUN=function(x){
  y<-substring(x,1,4)
  quarter<-substring(x,6,7)
  quarterDayMon<-switch(quarter,Q1="-01-01",Q2="-04-01",Q3="-07-01",Q4="-10-01")
  return(paste(y,quarterDayMon,sep=""))
  })
  
  return(forecastResults)
}

results<-rxExec(runForecasts, bDat.woNAPA, pmi)
resultsfiltered <- results$rxElem1
View(resultsfiltered)

# write.csv(results, file="results.csv")
# rxHadoopCopyFromLocal("results.csv", dest = "wasb://analytics@aapocblob.blob.core.windows.net/forecasts/data/")