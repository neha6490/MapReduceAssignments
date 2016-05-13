require(ggplot2)
require(dplyr)



files <- dir("/home/neha/test/outputs", pattern=".csv", full.names = TRUE)
outputs <- lapply(files, read.csv, header=FALSE, sep=",")
output <- bind_rows(outputs)
colnames(output) <- c("category","type","price")
library(ggplot2)
ggplot(data =output,aes(x=output$category,y=output$price,color=output$type,group=output$type))+geom_line()+geom_point(size=2)
plot1 <- ggplot(data =output,aes(x=output$category,y=output$price,color=output$type,group=output$type))+geom_line()+geom_point(size=2)
ggsave(filename = "Rplot.pdf",plot = plot1)
