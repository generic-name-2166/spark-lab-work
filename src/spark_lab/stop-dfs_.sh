#!/bin/bash
echo "Stopping HDFS"
su - hadoop -c "/usr/local/hadoop/hadoop-3.3.6/sbin/stop-dfs.sh" 
# exit
