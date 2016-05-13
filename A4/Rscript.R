require(dplyr)

args <- commandArgs(TRUE)
srcFile <- args[1]
ouputFile <- args[2]
files <- dir(srcFile, pattern='part-r-', full.names = TRUE)
outputs <- lapply(files, read.csv, header=FALSE, sep="\t")
data <- bind_rows(outputs)
names(data) <- c("Carrier","Time","Distance","Price")
vectorT <- c()
vectorD <- c()
mapT <- new.env(hash=T, parent=emptyenv())
mapD <- new.env(hash=T, parent=emptyenv())
for(i in unique(data$Carrier)){
  #extract data for a particular carrier
  dataCarrier <- data[which(data$Carrier == i), ]
  
  #linear regression
  dataLRT <- lm(dataCarrier$Price ~ dataCarrier$Time)
  dataLRD <- lm(dataCarrier$Price ~ dataCarrier$Distance)
  
  #find mean, slope and intercept and get a value that you can compare to find cheapest airline
  meanTimeCarrier <- mean(data$Time)*dataLRT$coefficients[2] + dataLRT$coefficients[1]
  meanDistanceCarrier <- mean(data$Distance)* dataLRD$coefficients[2] + dataLRD$coefficients[1]
  
  key <- i
  #time map
  mapT[[key]] = meanTimeCarrier
  #distance map
  mapD[[key]] = meanDistanceCarrier
  
  sm1 <- summary(dataLRT)
  sm2 <- summary(dataLRD)
  
  #find mean squared error
  t <- mean(sm1$residuals^2)
  d <- mean(sm2$residuals^2)
  
  #choosing whether we want to select distance or time as best measurement
  if(t < d){
    vectorT <- c(vectorT,t)
  }
  else{
    vectorD <- c(vectorD,d)
  }
  
  #create png files out of graphs
  png(paste(ouputFile,i,".png"))
  
  par(mfrow=c(2,1))
  
  plot(dataCarrier$Time,dataCarrier$Price,main = (paste("Time Vs Price for Carrier",sep=" ",i)))
  abline(dataLRT,col='red')
  
  plot(dataCarrier$Distance,dataCarrier$Price,main = (paste("Time Vs Price for Carrier",sep=" ",i)))
  abline(dataLRD,col='red')
  
  dev.off()
}
#search which is the cheapest airline
sorting <- c()
string <- ""
if(length(vectorT) > length(vectorD)){
  #time
  min <- 99999
  carrierName <- "none"
  for(i in unique(data$Carrier)){
    if(mapT[[i]] < min){
      min <- mapT[[i]]
      carrierName <- i
    }
    string <- paste(mapT[[i]],sep = ",",i)
    sorting <- c(sorting,string)
  }
}else{
  min <- 99999
  carrierName <- "none"
  for(i in unique(data$Carrier)){
    if(mapD[[i]] < min){
      min <- mapD[[i]]
      carrierName <- i
    }
    string <- paste(mapD[[i]],sep = ",",i)
    sorting <- c(sorting,string)
  }
}
sortedArray <- sort(sorting)
printArray <- paste(sortedArray,sep = ",",rank(sortedArray))
printArray
carrierName
