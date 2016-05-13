READ ME: 
===================================================================================================================================================================================
Understanding the Program and its Flow:
===================================================================================================================================================================================
Arguments: Enter the arguments.

Argument 0: The path where all the input files are present. To run the program for inputs of various sizes, create new input directories of 
			different sizes and change them accordingly in the Make file. The path given as args[0] gives the directory where all the files are present.

Argument 1: The argument is given as the path where the output should be stored.
	
The mapper begins reading each line of the sample data set provided to it and passes it to the sanity test. If the data passes the sanity test, read the values of average price, 
time and distance.

In the mapper we have made a custom object for each carrier which consist of the average price, distance and time. This value is then passed to the reducer.

The reducer sends the value to the output file to be read by R for linear regression.


IN R:
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

In R we calculate the linear regression where we try to predict the price for each airline against distance or time.

For the R program we first read the output file into a vector and assign a cloumn name to each value in the order of carrier, time, distance and price.

We have then looped for each unique carrier and performed linear regression on them using the lm function which finds the best possible line that fits the data points
in the set.

The lm function computes all the values of linear regression and we can predict the value for each unique airline using the simple line formula of y = mx + c where m = slope 
and c = intercept from thr values entered in the hashmap for each unique airline

We then computed the mean sqaure error and based on the lesser values of the mean square error we added it to the respective vectors. The vector with the lesser size seemed
like the better choice of explanatory variable. In out case we found it to be time and we decided to predict prices based on time.

We have also added the prices and distance for each unique carrier in to an hashmap and then sorted the hashmap to find the minimum  value for each airline. The sorting is 
done by finding the minimum price for each airline where predicted price is calculated using the mean value for the explanatory variable.


==========================================================================================================
Requirements:
==========================================================================================================
Linux or OS X
jq (for emr)
jdk 1.7+

==========================================================================================================
How to run
==========================================================================================================
There is Makefile using which you can run the program.
Our main file is HW4.java. For compilation please replace hadoop jar versions according to your hadoop version. 
My version of hadoop is 2.6.3
You will need to install jq in order to run the make emr as there is one script in it which needs jq.


Command : make pseudo : This command will run the program on pseudo distributed environment. Get the output from hdfs and
		  run the Rscript on it. Then it also creates a markdown file.
		  
		  In make pseudo you need to mention proper folder paths for input/output according to your file system.
		  
		  Following commands inside the make pseudo needs input/output folder paths.
		  
		  1)hadoop dfs -copyFromLocal <source of input files> <destination in hdfs>
		  2)hadoop jar job.jar HW4 <hdfs input path> <hdfs output path>
		  3)hadoop fs -get <hdfs output path>
		  4)Rscript Rscript.R <folder for input to rscript> <folder for saving output of rscript>
		  5)Rscript -e "rmarkdown::render('Assignment04.Rmd')" <folder for input to rscript>
		  
		  
		  make emr : This command will run the program on AWS cluster. This command will first create a bucket and will also create all
		  required folders.
		  You need to change output folder and input folder according to your file system.
		  
		  Following commands inside the make emr needs input/output folder paths
		  1)sh linear.sh <folder for output of the linear.sh script>
		  2)Rscript Rscript.R <folder for input to rscript which is same as output folder of .sh script> <folder for saving output of rscript>
		  3)Rscript -e "rmarkdown::render('Assignment04.Rmd')" <folder for input to rscript>

=========================================================================================================================================================================================================
To run Markdown separately:

Note : 	Please note it takes a long time to produce the pdf.

Requirements : TexLive (Linux)
	       MacTex (OSX)
	       Latest version of Pandoc

If you are opening the markdown in Rstudio replace args1 with the path of the output file of the reducer.

It will generate a pdf document.

=========================================================================================================================================================================================================

		   
==========================================================================================================================================================================================================
Output: 
==========================================================================================================================================================================================================
14 Airlines active in 2015.
HA
EV
MQ
OO
US
B6
WN
UA
DL
NK
VX
AS
F9
AA
NK

From the reducer we computed values for all flights between 2010 to 2014 and displayed only those active in the year 2015 and found 13 of these airlines active.
HA
EV
MQ
OO
US
B6
WN
UA
DL
NK
VX
AS
F9
AA

From R when we ranked the airlines with the lowest possible prices as :
"133.675422313118,F9,1" 
"149.526289318186,AS,2"  
"151.667288523876,WN,3" 
"373.971274561225,HA,4" 
"392.939282815709,MQ,5" 
"396.226131863862,OO,6" 
"397.12937405042,VX,7" 
"398.681588284215,EV,8" 
"401.532313917293,B6,9" 
"405.427352808947,AA,10"
"528.159207055109,US,11"
"539.016284707695,DL,12"
"672.852910696981,UA,13"

Least expensive airline : F9

========================================================================================================================================================================================================
Distance vs Time?
=========================================================================================================================================================================================================
We applied linear regression for prices against time and distance. The mean square error for most airlines was better in the case of time as against distance.

We checked this manually and in 10 cases the mean square error for time was better.

We also computed this in R, we created vectors for distance and time and for each airline we checked whether mse for time or distance was lesser. If it was lesser for time we added it
to the time vector, else we added it to the distance vector. The size of the time vector was greater.

Looking at this result and through observation, we chose time to predict prices as opposed to distance.

==========================================================================================================================================================================================================
Conclusions:
==========================================================================================================================================================================================================		

We received 13(printed in the output) airlines from the reducer output which were active in 2015 and got values for the airlines from the year 2010 to 2014.

From R while doing linear regression, we drew the graphs for each airline against time and distance.

The mean square errors was consistently better for time and hence we decided to pick time as the explanatory variable and tried to predict price based on time rather than distance.

Once we found the correct explanatory variable we calculated the predicted prices by picking a mean value in the data as the constant value with which we can predict prices for all 
airlines and when we sorted 

Once we found the mean, we multiplied the slope with the mean value and added the intercept. For the 13 airlines we plotted their predicted prices (showed in the output) we found F9 to 
have the least value.
