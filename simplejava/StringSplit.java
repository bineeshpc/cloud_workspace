public class StringSplit { 
    public static void main(String args[]) 
    { 
        String str = "engineer|manager|data scientist|analyst"; 
        String[] arrOfStr = str.split("\\|"); // pipe should be escaped with \\
  
        for (String a : arrOfStr) 
            System.out.println(a.trim()); 
    } 
} 
