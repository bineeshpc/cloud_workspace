import java.io.IOException;
import java.util.HashMap;
import java.util.ArrayList;
import java.lang.StringBuilder;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/*

Find top 5 job titles who are having highest growth in applications.

We need to define growth
If the title T1 has the info like this
T, (2012, 1), (2013, 1), .. (2016, 2)

Compound interest formula

A = P ( 1 + R) ^ t

r = (A / P) ^ (1 / t) - 1

Use one map reduce to figure out growth rate
and then figure out top N from that with another map reduce.

Once we have data in this format we can keep a priority queue of size n
to figure out the global top n

olympics analogy in the course

*/

class TitleCountMapper
     extends Mapper<Object, Text, Text, Text>{
         private Text jobTitleText = new Text();
         private String jobTitle;
         private String year;
         private Text yearCount = new Text();
       
         public void map(Object key, Text value, Context context
                  ) throws IOException, InterruptedException {
                      String[] arrOfStr = value.toString().split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
                      jobTitle = arrOfStr[4].trim().replace("\"", "");
                      year = arrOfStr[7].trim();
                      jobTitleText.set(jobTitle);
                      yearCount.set(year + "|" + 1);
                      System.out.println("************************* "+ jobTitle + yearCount.toString());
                      context.write(jobTitleText, yearCount);
        }
}

class IntSumReducer
     extends Reducer<Text, Text, Text, Text> {
  
  private String year;
  private String title;
  private String[] value;
  private HashMap<String, Integer> yearCount;
  private StringBuilder sb;
  private Text text;
  
  public void reduce(Text key, Iterable<Text> values,
                     Context context
                     ) throws IOException, InterruptedException {
    
    yearCount = new HashMap<String, Integer>();
    sb = new StringBuilder();
    
    for (Text val : values) {
      value = val.toString().split("\\|");
      year = value[0];
      System.out.println("************************* "+ val.toString() + " " + year + " " + value[1]);
      if (yearCount.get(year) == null) {
          yearCount.put(year, Integer.parseInt(value[1]));
      } else {
          yearCount.put(year, yearCount.get(year) + 1);
      }
    }
    for(String s: yearCount.keySet()) {
        sb.append(s + "|" + yearCount.get(s));
    }
    text = new Text();
    text.set(sb.toString());
    context.write(key, text);
    
 }
}


public class Q2JobTitlesTop5Growth {

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Q2JobTitlesTop5Growth");
    job.setJarByClass(Q2JobTitlesTop5Growth.class);
    job.setMapperClass(TitleMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }

}
