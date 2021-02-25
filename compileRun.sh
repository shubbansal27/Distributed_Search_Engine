hdfs dfs -rm -r Index
sh ../clearAllLogs.sh
hadoop com.sun.tools.javac.Main InvertedIndex.java
jar cf inverted.jar InvertedIndex*.class 
hadoop jar inverted.jar InvertedIndex input Index ##-D mapred.map.tasks=10 -D mapred.reduce.tasks=3  
 







###cat container_1473720635276_0003_01_0000*/stdout


