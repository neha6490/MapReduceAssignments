require(dplyr) 
require(ggplot2)
args <- commandArgs(TRUE) 
srcFile <- args[1] 
outputFile <- args[2] 
files <- dir(srcFile, pattern='part-r-', full.names = TRUE) 
outputs <- lapply(files, read.csv, header=FALSE, sep="\t") 
data <- bind_rows(outputs) 
names(data) <- c("Carrier","Year","MissedConnectionsCount","Percentage") 
#create png files out of graphs 
png(outputFile) 

ggplot(data=data, aes(x=data$Carrier, y=data$MissedConnectionsCount,fill=data$Year)) +
  geom_bar(stat="identity")
dev.off()
