#!/bin/bash
echo "user"
su - hadoop -c "/usr/local/hadoop/hadoop-3.3.6/bin/hdfs dfs -mkdir -p /user/"
echo "lab"
su - hadoop -c "/usr/local/hadoop/hadoop-3.3.6/bin/hdfs dfs -mkdir -p /user/lab/"
echo "Copy data from local"
su - hadoop -c "/usr/local/hadoop/hadoop-3.3.6/bin/hdfs dfs -copyFromLocal /mnt/d/Projects1/PyProjects/spark-lab/src/spark_lab/u.data /user/lab/"
echo "Copy items from local"
su - hadoop -c "/usr/local/hadoop/hadoop-3.3.6/bin/hdfs dfs -copyFromLocal /mnt/d/Projects1/PyProjects/spark-lab/src/spark_lab/u.item /user/lab/"
