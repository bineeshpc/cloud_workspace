hadoop com.sun.tools.javac.Main TopN.java
jar cf topn.jar *.class
rm -rf socialnetwork_output
hadoop jar topn.jar TopN ./socialnetwork.txt socialnetwork_output