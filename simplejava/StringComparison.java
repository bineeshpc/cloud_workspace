class StringComparison
{ 
    // Your program begins with a call to main(). 
    // Prints "Hello, World" to the terminal window. 
    public static void main(String args[]) 
    { 
        final String one = "one one";
        String one_dash = "\"one one\"";
        System.out.println(one.equals(one_dash.replace("\"", ""))); 
    } 
} 