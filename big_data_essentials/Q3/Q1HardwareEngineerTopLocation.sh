export HADOOP_HEAPSIZE=4096
hadoop com.sun.tools.javac.Main Q1HardwareEngineerTopLocation.java
jar cf Q1HardwareEngineerTopLocation.jar Q1HardwareEngineerTopLocation*.class
rm -rf ../h-1b-visa-100-output
hadoop jar Q1HardwareEngineerTopLocation.jar Q1HardwareEngineerTopLocation ../h-1b-visa-100 ../h-1b-visa-100-output 