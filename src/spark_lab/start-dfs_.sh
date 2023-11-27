#!/bin/bash
# ssh localhost
echo "Formatting HDFS"
su - hadoop -c "/usr/local/hadoop/hadoop-3.3.6/bin/hdfs namenode -format" 
echo "Starting HDFS and server"
su - hadoop -c "/usr/local/hadoop/hadoop-3.3.6/sbin/start-dfs.sh" 
