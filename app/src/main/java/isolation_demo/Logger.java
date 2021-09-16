package isolation_demo;

public class Logger {
    static void print(String s){
        System.out.println(s);
    }

    static void error(String s){
        System.out.println("Error: " + s);
    }
}
