sh clearAllLogs.sh 
stop-all.sh

##node1
#ssh hduser@node1 "rm -rf mydata/data/*"
#ssh hduser@node1 "rm -rf mydata/name/*"
#ssh hduser@node1 "rm -rf mydata/tmp/*"

##node2
#ssh hduser@node2 "rm -rf mydata/data/*"
#ssh hduser@node2 "rm -rf mydata/name/*"
#ssh hduser@node2 "rm -rf mydata/tmp/*"

#node3
#ssh hduser@node3 "rm -rf mydata/data/*"
#ssh hduser@node3 "rm -rf mydata/data/*"
#ssh hduser@node3 "rm -rf mydata/data/*"

#node4
#ssh hduser@node4 "rm -rf mydata/data/*"
#ssh hduser@node4 "rm -rf mydata/data/*"
#ssh hduser@node4 "rm -rf mydata/data/*"

#node5
#ssh hduser@node5 "rm -rf mydata/data/*"
#ssh hduser@node5 "rm -rf mydata/data/*"
#ssh hduser@node5 "rm -rf mydata/data/*"


##node0
rm -rf mydata/data/*
rm -rf mydata/name/*
rm -rf mydata/tmp/*
hdfs namenode -format
start-all.sh
hdfs dfs -mkdir -p /user/hduser
hdfs dfs -put Lab1/input /user/hduser/
hdfs dfs -put util /user/hduser/


##node0
echo "\nNode0-Status"
jps
echo "\n"

##node1
#echo "\nNode1-Status"
#ssh hduser@node1 "jps"
#echo "\n"

#node2
#echo "\nNode2-status"
#ssh hduser@node2 "jps"
#echo "\n"

#node3
#echo "\nNode3-status"
#ssh hduser@node3 "jps"
#echo "\n"

#node4
#echo "\nNode4-status"
#ssh hduser@node4 "jps"
#echo "\n"


#node5
#echo "\nNode5-status"
#ssh hduser@node5 "jps"
#echo "\n"



#hdfs dfsadmin -safemode leave

