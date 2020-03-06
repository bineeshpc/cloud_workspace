hadoop com.sun.tools.javac.Main Q1DataEngineerIncreasing.java
jar cf Q1DataEngineerIncreasing.jar Q1DataEngineerIncreasing*.class
rm -rf ../h-1b-visa-100-output
hadoop jar Q1DataEngineerIncreasing.jar Q1DataEngineerIncreasing ../h-1b-visa-100 ../h-1b-visa-100-output