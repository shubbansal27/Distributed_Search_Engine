hdfs dfs -rm -r query_output
hdfs dfs -rm -r query_input

hdfs dfs -put query_input /user/hduser/query_input

sh ../clearAllLogs.sh
hadoop com.sun.tools.javac.Main Query_hadoop.java
jar cf Query_hadoop.jar Query_hadoop*.class 
hadoop jar Query_hadoop.jar Query_hadoop query_input query_output ##-D mapred.map.tasks=10 -D mapred.reduce.tasks=3  
 







###cat container_1473720635276_0003_01_0000*/stdout


