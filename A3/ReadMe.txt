READ ME: 
=================================================================================================================
Understanding the Program and its Flow:
=================================================================================================================
Arguments: Enter the arguments.

Argument 0: The path where all the input files are present. To run the program for inputs of various sizes, create new input directories of 
			different sizes and change them accordingly in the Make file. The path given as args[0] gives the directory where all the files are present.

Argument 1: The argument is given as the path where the output should be stored.
	
Argument 2: The argument takes the type of the program to be run. Example, sequential, multi threaded, pseudo. These options are configured in the Make File,.

Argument 3: The argument takes the category for the type of program to be run. Example, mean, median, fast median. These options are configured in the Make File.

Based on the combinations between Argument 2 and Argument 3 we can have 12 run configurations which will produce the output files with the time recorded for each configuration.

Explanation for Sequential Program: The sequential program reads each line from each file line by line. It then passes it to a sanity test.
If the record passes the sanity test, we read the year, month and the average price per airline. If the carrier is active in 2015, enter it to a global set
maintained for the sequential program. The sequential program maintains a hashmap with the carrier and month as a key as the list of prices as a value.
The mean, median and fast median is then computed using this hashmap.

Explanation for Multi Threaded Program: For the multi threaded program, we read each file in a separate thread. The records then pass through a sanity test and 
flights active in 2015 are then checked. If the flight is active the records are added to a hashmap similar to the sequential program and then each thread returns
its own hashmap. All the values of the hashmap are collected and added to a single hashmap. The hashmap containing all the values is used to calculate the median, mean and fast median.

Explanation for pseudo: The map reads the records passed to it. The value are then passes to a sanity test and if it passes the value, we read it and then 
create a custom object where we store the year, month and average price of the flight. The map then passes the value to the reducers as the carrier as a key and
the custom object of flight as a value.
We have created 3 reducers based on the choice of Argument 3 of mean ,median and fast median. Each reducer computes on of the following based on the choice.
In each reducer we have a hashmap for each month and an arraylist for the average months. As each reducer receives the records only for one carrier we set 
the month as a key and the average price is then appended as a list associated with each month.
Based on the choice, we calculate the mean, median or the fast median for each month.

Time Logs:
The logs for sequential program, pseudo and multi threaded is written to an output path from where it is used as an input for R.
Time logs are basically the execution time for a particular category of program (e.g. pseudo,sequential etc.) for executing 
particular type of operation (e.g. mean, median, fast median)

Difference in median and fast median: Fast median is always faster than median. We used quick selection approach for that.
We didn't use any approximation.

==========================================================================================================
Requirements:
==========================================================================================================
Linux or OS X
jq (for emr)
jdk 1.7+

==========================================================================================================
How to run
==========================================================================================================
There is MakeFile using which you can run the program. HW1.java is our main file.

Command : make format, make hstart and make neha commands are used to format hadoop hdfs.
		  You should not execute these commands and set up your own input and output folders.
		  'make program' command will be used to run programs. This will run a number of commands and run
		   12 configurations.
		   "sequential" "mean"
		   "sequential" "median"
		   "sequential" "fast median"
		   "multithreaded" "mean"
		   "multithreaded" "median"
		   "multithreaded" "fast median"
		   "pseudo" "mean"
		   "pseudo" "median"
		   "pseudo" "fast median"
		   "emr" "mean"
		   "emr" "median"
		   "emr" "fast median"
		   
		   Also, will log timings in a folder. 
		   Then R script will run and plot a graph based on the output values and generate a report.
		   
		   Before Running the command 'make program': 
		   Please mention paths of jars for hadoop according to your hadoop version. I have hadoop version 2.6.3 so
		   I have included jars according to my version.
		   
		   Please mention your file paths in make file. Also mention
		   your hdfs input and output folder paths.
		   
		   Also, in HW1.java file, please give path to the folder where you want to generate the output.
		   Constants name that you will have to edit according to your path : 
		   outputPath
		   
		   In cli.java please change the path of the output. This path must be same as the output folder 
		   you are mentioning for sequential, multithreaded etc. (As above)

		   Please add path to the output folder in MakeFile in make program. Mention your path, currently I have put
		   my path.(commands for Rscript, there are 2 commands).
		   
		   Note: All the outputs 'mCv' for sequential and multithreaded, will be printed in text files which will also get generated 			in the folder that you
		   have mentioned as output folder. For hadoop programs, output will get generated in hdfs as you know.
		   
==========================================================================================================
Output: 
==========================================================================================================
		After execution you will see all the timings logged in csv files and mCv (m identifies the month, 
		C is an airline active in 2015 for top 10 airlines and v is value of mean/median/fast median in txt files.
		
		Here is the sample output csv of benchmarking harness (time in milliseconds):
		

		For very small data:
		
		Sequential Analysis:
		sequential,fast median,82778
		sequential,median,87611
		sequential,mean,83834

		Multi Threaded Program:
		multithreaded,fast median,74646
		multithreaded,median,75268
		multithreaded,mean,76233
		
		Psuedo Distributed Analysis:
		pseudo,fast median,95537
		pseudo,median,104036
		pseudo,mean,110469
		
		Distributed Analysis:
		emr,fast median,1652150
		emr,median,1665992
		emr,mean,1647120
------------------------------------------------------------------------------------------------------

		For original data:
		
		Sequential Analysis: 
		sequential,fast median,413890
		sequential,median,438055
		sequential,mean,419790

		Multi Threaded Program:
		multithreaded,fast median,373230
		multithreaded,median,376340
		multithreaded,mean,386845
		
		Psuedo Distributed Analysis:
		pseudo,fast median,520180
		pseudo,median,525453
		pseudo,mean,563454
		
		Distributed Analysis:
		emr,fast median,1753155
		emr,median,1867602
		emr,mean,1847325
		
--------------------------------------------------------------------------------------------------------------



==========================================================================================================
Conclusions:
==========================================================================================================		
		Small Data:
		For small amount of data, we noticed that multi threaded programs ran the fastest. Followed by the sequential Program and psuedo distributed program. The distributed program gave the worst performance.

		Original Data:
		For the original amount of data given, multi threaded program still gave us the best performance, followed by psuedo distributed program and then sequential program. Distributed program still ran the worst.

		Replicating the data thrice:
		For replicating the data thrice, psuedo distributed mode ran the fastest, followed by multi threaded program and then sequential. The distributed mode still gave us the slowest performance.

		Replicating the data eight times:
		In this case, the distributed mode ran the fasest, followed by the psuedo distributed code and then the multi threaded program. The worst performance was by the sequential mode.

Therefore what we understood is that distributed mode works best for very very large amounts of data. For smaller amounts of data, psuedo distributed mode is sufficient. If the data happens to be small, its best to use a multi threaded program.   

Fast Median vs Median:
		We used a method which calculates fast median in a linear way similar to quick sort, hence the time performance for fast median was always faster than the median calculation.
		Also, we always found that the values of fast median and median were always the same. Hence our calculation if fast median was accurate.

		   
