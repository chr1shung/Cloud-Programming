rm -r class/*
javac -classpath  $HADOOP_HOME/share/hadoop/mapreduce/hadoop-mapreduce-client-core-2.7.2.jar:$HADOOP_HOME/share/hadoop/common/hadoop-common-2.7.2.jar -d class PageRank/*
jar -cvf PageRank.jar -C class .
