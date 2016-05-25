hdfs dfs -rm -r p1

hadoop jar PageRank.jar pageRank.PageRank /shared/HW2/sample-in/input-100M p1
hdfs dfs -cat p1/part-*
