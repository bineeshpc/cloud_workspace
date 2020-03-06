package topn;

import org.apache.hadoop.io.Text;

public class User implements Comparable<User> {
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