require(dplyr)
require(ggplot2)

args <- commandArgs(TRUE)
srcFile <- args[1]
outPutFile <-  args[2]
files <- dir(srcFile, pattern='part-r-', full.names = TRUE)


outputs <- lapply(files, read.csv, header=FALSE, sep="\t")
output <- bind_rows(outputs)

colnames(output) <- c("carrier","price","month","frequency")
library(ggplot2)
sorted <- output[order(-output$frequency),]

sorted$rank <- rank(-sorted$frequency,ties.method="min")

sorted$drank <- rep(1:length(rle(sorted$rank)$values),rle(sorted$rank)$lengths)

sortedNew <- sorted[sorted$drank <= 10,]

plot1 <- ggplot(data=sortedNew,aes(x=sortedNew$month,y=sortedNew$price,color=sortedNew$carrier)) + geom_line() + scale_color_manual(name="Top 10 Airlines",values = c("red","green","blue","orange","purple","pink","yellow","black","brown","maroon")) + ggtitle("Average price of airlines per month") + labs(x="Month(1-12)",y="Average Price(USD)")

plot1

ggsave(filename = outPutFile, plot = plot1)
