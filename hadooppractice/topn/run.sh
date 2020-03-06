hadoop com.sun.tools.javac.Main User.java Map.java Reduce.java Main.java
jar cf Main.jar *.class
rm -rf socialnetwork_output
hadoop jar Main.jar Main ./socialnetwork.txt socialnetwork_output