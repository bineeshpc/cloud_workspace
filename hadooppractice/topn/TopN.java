

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;


import java.util.PriorityQueue;


class User implements Comparable<User> {
    private int followers;
    private Text record;
    
    public User(int followers, Text record) {
        this.followers = followers;
        this.record = record;
    }
    
    public int getFollowers() {
        return followers;
    }
    
    public Text getRecord() {
        return record;
    }
    
    @Override
    public int compareTo(User user) {
        return this.followers - user.followers;
    }
}

class Map extends Mapper<LongWritable, Text, NullWritable, Text> {
    
    private int n = 3;
    private PriorityQueue<User> followersPriorityQueue = new PriorityQueue<>();
    
    public void map(LongWritable key, Text value, Context context) {
        String line = value.toString();
        String[] data = line.split("\t");
       
        Integer followers = Integer.parseInt(data[1]);
        
        User user = followersPriorityQueue.peek();
        
        if (followersPriorityQueue.size() <= n || 
        followers > user.getFollowers()) {
            
            followersPriorityQueue.add(new User(followers, new Text(value)));
            
            if (followersPriorityQueue.size() > n) {
                
                followersPriorityQueue.poll();  // delete smallest item
            }
        }
    }
    
    // Executed after all map operations are done
    @Override
    protected void cleanup(Context context) throws IOException,
    InterruptedException {
        while (!followersPriorityQueue.isEmpty()) {
            
            context.write(NullWritable.get(),
            followersPriorityQueue.poll().getRecord()
            );
        }
    }
}

class Reduce extends Reducer<NullWritable,  Text, NullWritable, Text> {
    private int n = 3;
    private PriorityQueue<User> followersPriorityQueue = new PriorityQueue<>();
    
    @Override
    public void reduce(NullWritable key, 
                        Iterable<Text> values,
                        Context context) 
                        throws IOException, InterruptedException{
                         
        for (Text value: values) {
            
            String line = value.toString();
            String[] data = line.split("\t");
            Integer followers = Integer.parseInt(data[1]);
            
            User user = followersPriorityQueue.peek();
            
            if (followersPriorityQueue.size() <= n || 
            followers > user.getFollowers()) {
                
                followersPriorityQueue.add(new User(followers, new Text(value)));
                
                if (followersPriorityQueue.size() > n) {
                     
                    followersPriorityQueue.poll();  // delete smallest item
                }
            }
        }
        
        while  (!followersPriorityQueue.isEmpty()) {
            //System.out.println("Reducer *******: " + followersPriorityQueue.peek());
            context.write(NullWritable.get(),
            followersPriorityQueue.poll().getRecord());
        }
    }
}

public class TopN extends Configured implements Tool {
 
    @Override
    public int run(String[] args) throws Exception{
        Job job = Job.getInstance(getConf());
        job.setJobName("TopN Records");
        job.setJarByClass(TopN.class);
        
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        return job.waitForCompletion(true) ? 0 : 1;
    } 
    
    public static void main(String[] args) throws Exception {
    
        int exitCode = ToolRunner.run(new TopN(), args);
        System.exit(exitCode);
    }
}