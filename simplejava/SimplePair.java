
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

class SimplePair 
{ 
    // Your program begins with a call to main(). 
    // Prints "Hello, World" to the terminal window. 
    public static void main(String args[]) 
    { 
        Pair pair = new Pair("Magnus", 1);
        System.out.println(pair.getKey() + pair.getValue()); 
    } 
} 