import java.util.*;
import java.io.*;
import java.net.*;

public class NameServer{
    public static Socket socket = null;
    static HashMap<Integer, String> keyValues = new HashMap<>();
    public static void main(String args[]) throws Exception{
        InetAddress localHost = InetAddress.getLocalHost();
        String hostServer = localHost.getHostAddress();
        String configPath = args[0];
        File config = new File(configPath);
        Scanner sc = new Scanner(config);
        ServerInfo ns = new ServerInfo();
        ns.id= sc.nextInt();
        ns.listeningPort = sc.nextInt();
        ns.bootstrapip = sc.next();
        ns.bootstrapPort = sc.nextInt();
        ns.ip = hostServer;
        Scanner input = new Scanner(System.in);
        String msg = "";
        int keyin = 0;
        String inputUser = "";
        while(true){
            inputUser = input.next();
            if(inputUser.equals("enter")){
                socket = new Socket(ns.bootstrapip, ns.bootstrapPort);
                DataInputStream istream = new DataInputStream(socket.getInputStream());
                DataOutputStream ostream = new DataOutputStream(socket.getOutputStream());
                ostream.writeUTF("enter");
                ostream.writeUTF(Integer.toString(ns.id));
                ostream.writeUTF(Integer.toString(ns.listeningPort));
                ostream.writeUTF(ns.ip);
                while(true){
                    keyin = Integer.parseInt(istream.readUTF());
                    if(keyin == -1){
                        break;
                    }
                    msg = istream.readUTF();
                    keyValues.put(keyin, msg);
                }
                System.out.println("stops here");
                ns.predissesorid = Integer.parseInt(istream.readUTF());
                ns.predissesorPort = Integer.parseInt(istream.readUTF());
                ns.predisessorip = istream.readUTF();
                System.out.println("here: "+ns.predisessorip);
                System.out.println("here: "+ns.predissesorPort);
                ns.successorid = Integer.parseInt(istream.readUTF());
                System.out.println("here: "+ns.successorid);
                ns.successorport = Integer.parseInt(istream.readUTF());
                System.out.println("here: "+ns.successorid);
                ns.successorip = istream.readUTF();
                System.out.println("ID= "+ns.id);
                System.out.println("lemon 434 = "+keyValues.get(434));
                System.out.println("beetroot 325 = "+keyValues.get(325));
                System.out.println("cherry 288 = "+keyValues.get(288));

                NameServerThread thread = new NameServerThread(ns, keyValues);
                thread.start();
                socket.close();
            }
            if(inputUser.equals("exit")){
                socket = new Socket(ns.bootstrapip, ns.bootstrapPort);
                DataInputStream istream = new DataInputStream(socket.getInputStream());
                DataOutputStream ostream = new DataOutputStream(socket.getOutputStream());
                ostream.writeUTF("exit");
                ostream.writeUTF(Integer.toString(ns.id));
                ostream.writeUTF(Integer.toString(ns.listeningPort));
                ostream.writeUTF(ns.ip);
            }
        }
    }


}