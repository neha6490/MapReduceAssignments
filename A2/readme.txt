
SOFTWARE REQUIREMENTS

DEVELOPMENT TOOLS:

-Oracle JDK version 1.7
-Hadoop version 2.7.2
-R, RStudio and all packages listed in 'R Configuration' section

TOOLS DEPLOYING AND RUNNING:

-Linux Command Line environment
-AWS Command Line Interface 
-A running pseudo-distributed hadoop setup on the local machine
-A running setup for AWS

VARIABLES:

-The $JAVA_HOME needs to point to the root of the java directory
-THE $HADOOP_HOME needs to be set to the directory where the hadoop installation lives
-Make sure that the $PATH contains the hadoop bin and sbin

FOR AWS:
-Make sure that you have a S3 bucket and input and output folders in the bucket
-Make sure that AWS CLI is setup with the required authentication
Please configure authentication keys using 'aws configure' command. Set the region as 'us-west-2a'.

FOR R:

-R helps us plot graphs for interpretated data
-For the given project, the RCode runs in a '.rmd' file that contains R code to 
-dplyr
-ggplot2
-rmarkdown
-pandoc
(please make sure the above R packages are installed)

EXECUTION OF PROGRAM:

-Extract the project tar.gz into a folder named 'test'.
-Inside the Makefile, replace <BUCKET_NAME> with the S3 bucket name that you have created
-Assuming you have the input data (the 'all' folder):
   To execute in pseudo-distributed mode - 
   -Overwrite the all folder in the hdfs, and create a fresh folder following- /user/<LOCAL_USER>/input/all
   -Copy all the data files from all into this folder
   -Confirm that the /user/<LOCAL_USER> does not contain 'output' folder
   -run: make pseudo which will compile, and run the program

   To execute on AWS:
   -Confirm that 'input/all' folder does not exist in the S3 bucket, as the make command will take care of it
   -Confirm that 'output' folder does not exist in the S3 bucket, as the program will take care of it
   -Use command - make cloud
   -Manually get the output files from the console after the job completes. As there is no way to know if aws has 
    completed the job, we could not write any command to get the output.

