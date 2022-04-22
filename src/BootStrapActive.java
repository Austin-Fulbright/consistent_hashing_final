import java.io.*;
import java.util.*;
import java.net.*;


public class BootStrapActive extends Thread{

    public BootStrapActive(){

    }

    public void run(){
        String userInput = "";
        int key = 0;
        String value = "";
        Scanner sc = new Scanner(System.in);
        while(true){
            userInput = sc.nextLine();
            if(userInput.equals("lookup key")){
                key = sc.nextInt();
                BootStrapServer.lookup(key);
            }
            else if(userInput.equals("insert key value")){
                key = sc.nextInt();
                value = sc.next();
                BootStrapServer.insertKey(key, value);
                
            }
            else if(userInput.equals("delete key")){
                key = sc.nextInt();
                BootStrapServer.delete(key);
            }
    }

    }

}