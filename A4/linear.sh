#!/bin/bash
#Author : Joy, Neha
i=0
output=$1
eval job_id=`aws emr create-cluster --name "testMytestHello" --release-label emr-4.3.0 --instance-groups InstanceGroupType=Master,InstanceCount=1,InstanceType=c1.medium InstanceGroupType=CORE,InstanceCount=2,InstanceType=c1.medium --steps Type=CUSTOM_JAR,Name="job",ActionOnFailure=CONTINUE,Jar=s3://testMytest/job.jar,MainClass=HW4,Args=[s3://testMytest/input/all,s3://testMytest/output] --auto-terminate --log-uri s3://testMytest/log --service-role EMR_DefaultRole --ec2-attributes InstanceProfile=EMR_EC2_DefaultRole,AvailabilityZone=us-east-1a --enable-debugging | jq ".ClusterId"`

status=`aws emr describe-cluster --cluster-id "$job_id" | jq ".Cluster.InstanceGroups[$i].Status.State"`
echo $status
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
aws s3 sync s3://testMytest/output $1
echo "The job has terminated and output downloaded"
