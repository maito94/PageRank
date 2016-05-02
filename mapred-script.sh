KEY_NAME=cs5300-key-mag399;
S3_BUCKET=edu-cornell-cs-cs5300s16-mag399-proj2;

PATH_TO_JAR=s3://${S3_BUCKET}/jar/pagerank.jar;
PATH_TO_LOGS=s3n://${S3_BUCKET}/logs/
PATH_TO_EDGES=input/edges.txt;
PATH_TO_BLOCKS=input/blocks.txt;
PATH_TO_RESULTS=results_with_blocks.txt; # This is where information pertaining to map reduce jobs will be located
PATH_TO_HADOOP_OUT=output/; #make sure this folder does not exist
NUMBER_OF_NODES=685230;
MAX_NUMBER_OF_MAPRED=10;
INSTANCE_TYPE=m1.medium;
INSTANCE_N=5;

NO_BLOCK_IMPLEMENTATION=["${NUMBER_OF_NODES}","${MAX_NUMBER_OF_MAPRED}","${S3_BUCKET}","${PATH_TO_EDGES}","${PATH_TO_HADOOP_OUT}","${PATH_TO_RESULTS}"];
BLOCK_IMPLEMENTATION=["${NUMBER_OF_NODES}","${MAX_NUMBER_OF_MAPRED}","${S3_BUCKET}","${PATH_TO_EDGES}","${PATH_TO_HADOOP_OUT}","${PATH_TO_RESULTS}","${PATH_TO_BLOCKS}"];

aws emr create-cluster --name "PageRank Cluster" --release-label emr-4.6.0 \
--applications Name=Hadoop --use-default-roles \
--enable-debugging --log-uri "${PATH_TO_LOGS}" \
--ec2-attributes KeyName=${KEY_NAME} --instance-type ${INSTANCE_TYPE} --instance-count ${INSTANCE_N} \
--steps Type=CUSTOM_JAR,Name="Custom JAR Step",ActionOnFailure=CONTINUE,Jar=${PATH_TO_JAR},\
Args=${NO_BLOCK_IMPLEMENTATION} \
--auto-terminate \
--region us-east-1
