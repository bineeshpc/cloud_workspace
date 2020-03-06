package simplepackage;

class SimplePackage {
    public String getName(){
        return "SimplePackage";
    }
}

class TestPackage{
    
    
    public static void main(String args[]) {
        SimplePackage sp;
        sp = new SimplePackage();
        System.out.println(sp.getName());
    }
}