export HADOOP_HEAPSIZE=4096
hadoop com.sun.tools.javac.Main WordCount.java
jar cf wc.jar WordCount*.class
echo "Hello Hadoop Goodbye Hadoop" > wordcount_test.txt
hadoop jar wc.jar WordCount ./wordcount_test.txt ./output