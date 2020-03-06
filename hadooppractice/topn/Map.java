package topn;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.PriorityQueue;

public class Map extends Mapper<LongWritable, Text, NullWritable, Text> {
    
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
    protected void Cleanup(Context context) throws IOException,
    InterruptedException {
        while (!followersPriorityQueue.isEmpty()) {
            context.write(NullWritable.get(),
            followersPriorityQueue.poll().getRecord()
            );
        }
    }
}