import com.opencsv.CSVReader;
import java.util.Arrays;
import java.util.List;
import java.io.StringReader;

class StringCSVReader 
{ 
    // Your program begins with a call to main(). 
    // Prints "Hello, World" to the terminal window. 
    public static void main(String args[]) 
    { 
        CSVReader reader = new CSVReader(new StringReader("one,two,three"));
        
        List<String[]> allRows = reader.readAll();
        for(String[] row : allRows){
            System.out.println(Arrays.toString(row));
        }
    } 
} 
