import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


class Pair {
    private String key;
    private int value;
    
    public Pair(String key, int value) {
        this.key = key;
        this.value = value;
    }
    public String getKey(){
        return this.key;
    }
    
    public int getValue() {
        return this.value;
    }
    
    public void setKey(String key, int value) {
        this.key = key;
        this.value = value;
    }
}


public class Q2JobTitlesTop5Growth {

  public static class DataEngineerMapper
       extends Mapper<Object, Text, Text, Pair>{
           private Text jobTitleText = new Text();
           private String jobTitle;
           private String year;
           private Pair yearCount;
           
           public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
                        String[] arrOfStr = value.toString().split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
                        jobTitle = arrOfStr[4].trim().replace("\"", "");
                        year = arrOfStr[7].trim();
                        jobTitleText.set(jobTitle);
                        yearCount = new Pair(year, 1);
                        context.write(jobTitleText, yearCount);
          }
  }

  public static class IntSumReducer
       extends Reducer<Text,IntWritable,Text,Pair> {
    
    private Pair yearCount;
    private String year;
    private int value;
    public void reduce(Text key, Iterable<Pair> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (Pair pair : values) {
        year = pair.getKey();
        value = pair.getValue();
        sum += value;
      }
      yearCount = new Pair(year, sum);
      
      context.write(key, yearCount);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Q2JobTitlesTop5Growth");
    job.setJarByClass(Q2JobTitlesTop5Growth.class);
    job.setMapperClass(DataEngineerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
