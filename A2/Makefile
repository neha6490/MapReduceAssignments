format: 
	hdfs namenode -format

hstart:
	start-dfs.sh
	start-yarn.sh
	mr-jobhistory-daemon.sh start historyserver


hstop:
	mr-jobhistory-daemon.sh stop historyserver 
	stop-yarn.sh
	stop-dfs.sh

neha:
	hadoop fs -mkdir -p /user/neha
	hadoop fs -mkdir -p /user/neha/input
pseudo:
	javac -cp /usr/local/hadoop/share/hadoop/mapreduce/lib/hadoop-annotations-2.6.3.jar:/usr/local/hadoop/share/hadoop/common/hadoop-common-2.6.3.jar:/usr/local/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-client-core-2.6.3.jar  HW1.java
	jar cvf job.jar *.class
	export HADOOP_CLASSPATH=.:`hadoop classpath`
	hadoop fs -put all input	
	hadoop jar job.jar HW1 input/all output
	hadoop fs -get output 
	Rscript hw2.r output plot.png

emr:
	javac -cp /usr/local/hadoop/share/hadoop/mapreduce/lib/hadoop-annotations-2.6.3.jar:/usr/local/hadoop/share/hadoop/common/hadoop-common-2.6.3.jar:/usr/local/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-client-core-2.6.3.jar  HW1.java
	jar cvf job.jar *.class
	aws s3 ls s3://<BUCKET_NAME>
	aws s3 mb s3://<BUCKET_NAME>
	aws s3 cp all/ s3://<BUCKET_NAME>/input/all --recursive
	aws s3 cp job.jar s3://<BUCKET_NAME>
	aws emr create-cluster --name "test-Cluster" --release-label emr-4.3.0 --instance-groups InstanceGroupType=Master,InstanceCount=1,InstanceType=c1.medium InstanceGroupType=CORE,InstanceCount=2,InstanceType=c1.medium --steps Type=CUSTOM_JAR,Name="ClusterAnalysis",ActionOnFailure=CONTINUE,Jar=s3://<BUCKET_NAME>/job.jar,MainClass=HW1,Args=[s3://<BUCKET_NAME>/input/all,s3://<BUCKET_NAME>/output/] --auto-terminate --log-uri s3://<BUCKET_NAME>/log --service-role EMR_DefaultRole --ec2-attributes InstanceProfile=EMR_EC2_DefaultRole,AvailabilityZone=us-east-1a --enable-debugging
	
	 
