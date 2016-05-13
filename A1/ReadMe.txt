Please go through the following instructions to run the assignment:

HOW TO RUN
-----------------------------------------------------------------------------------------
1. Java 1.8 to compile the source.
2. Extract the tar file.
3. Import the Gradle Project.


PACKAGE INCLUDES
-----------------------------------------------------------------------------------------
1. Source code (src\ThreadedAnalysis.java)
2. build.gradle


Executing the program
-------------------------------------------------------------------------------------------
1. Import the Gradle Project in Eclipse. (Make sure you click on "Build Model")
2. The Gradle build should successfully pull "commons-csv-1.2.jar"


Input for the program :
-------------------------------------------------------------------------------------------
Program expects 1 or 2 arguments as an input.
(if you provide wrong path, you might get NullPointerException and/or FileNotFound exception)
If you provide only 1 argument : Program will take that 1 argument as the path to the directory.
If you provide 2 arguments : 1st argument will be assigned to flag which will decide if the program should run in
parallel mode. If you provide "no" then it will run sequentially. If you provide any other value, program will run in parallel
mode.
2nd argument will be taken as the path to the directory.

Example input: -p C:\Users\Neha\Downloads\all 
this will run the program in parallel mode.


Output of the program :
---------------------------------------------------------------------------------------------
K:60457
F:12598804
F9 135.36591450319727 82.11
WN 139.60641703046363 117.0
AS 203.15007738510147 171.6
HA 278.32426992088364 122.85
OO 285.0457001584724 257.4
MQ 286.3437274460123 267.75
EV 293.60520226837616 273.06
NK 487.5081489117994 469.36
AA 500.6120137610222 467.5
B6 500.7641805675014 462.48
DL 570.1871367034228 480.51
US 572.2144863373709 469.17
VX 638.0572908239515 594.0
UA 957.4302125436875 865.44

* For K and F values I am not calculating number of header lines. Also, if Avg ticket price is empty, I am considering it as a corrupt line.