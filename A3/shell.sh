rm -rf <output_path>
args1=$1
eval job_id=`aws emr create-cluster --name "test" --release-label emr-4.3.0 --instance-groups InstanceGroupType=Master,InstanceCount=1,InstanceType=c1.medium InstanceGroupType=CORE,InstanceCount=2,InstanceType=c1.medium --steps Type=CUSTOM_JAR,Name="Tests",ActionOnFailure=CONTINUE,Jar=s3://<BucketName>/job.jar,MainClass=HW1,Args=[$1,-emr,s3://<BucketName>/input/all,s3://<BucketName>/output/] --auto-terminate --log-uri s3://<BucketName>/log --service-role EMR_DefaultRole --ec2-attributes InstanceProfile=EMR_EC2_DefaultRole,AvailabilityZone=us-east-1a --enable-debugging | jq ".ClusterId"`
aws s3 rm s3://<BucketName>/log --recursive
aws s3 rm s3://<BucketName>/output --recursive
status=`aws emr describe-cluster --cluster-id "$job_id" | jq ".Cluster.InstanceGroups[$i].Status.State"`
flag="False"
while [ $flag == "False" ]
do
	sleep 5
	status=`aws emr describe-cluster --cluster-id "$job_id" | jq ".Cluster.InstanceGroups[$i].Status.State"`
	
	if [ $status = '"TERMINATED"' ]; then
		flag="True"
	else
		flag="False"
	fi
done
aws s3 sync s3://<BucketName>/output <output_path>
aws s3 sync s3://<BucketName>/log/$job_id/steps/ <output_path>/logs/
cd <Path_to_java_file>
javac cli.java
java cli <output_path>/logs $args1

