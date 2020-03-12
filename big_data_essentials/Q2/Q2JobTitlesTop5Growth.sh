export HADOOP_HEAPSIZE=4096
hadoop com.sun.tools.javac.Main Q2JobTitlesTop5Growth.java
jar cf Q2JobTitlesTop5Growth.jar *.class
rm -rf ../h-1b-visa-100-output
hadoop jar Q2JobTitlesTop5Growth.jar Q2JobTitlesTop5Growth ../h-1b-visa-100 ../h-1b-visa-100-output