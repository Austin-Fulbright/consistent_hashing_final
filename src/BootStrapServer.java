import java.io.*;
import java.net.*;
import java.util.*;


public class BootStrapServer{
    
    static HashMap<Integer, String> keyValues = new HashMap<>();
    static int id = 0;
    static int socketNumber = 0;
    static int predid = 0;
    static int succid = 0;
    static int predListening = 0;
    static int succListening = 0;
    static Socket socket = null;
    static ServerInfo bs = new ServerInfo();
    public static ArrayList<Integer> serverIDS = new ArrayList<>();
    public BootStrapServer(){

    }

    public static void addToBootStrap(DataInputStream istreamRecieve){
        
        try{
            String msg = "";
            int i = 0;
            String v = ""; 

            while(true){
                msg = istreamRecieve.readUTF();
                System.out.println(msg);
                if(msg.equals("-1")){
                    break;
                }
                i = Integer.parseInt(msg);
                msg = istreamRecieve.readUTF();
                v = msg;
                keyValues.put(i, v);
            }

        }catch(Exception e){

        }
    }
    public static void lookup(int lookKey){
        try{
        System.out.println(bs.predissesorid);
        if(lookKey>bs.predissesorid||bs.predissesorid==0){
            if(keyValues.containsKey(lookKey)){
            System.out.println(keyValues.get(lookKey));
            System.out.println("visited: 0");
            }
            else{
                System.out.println("key not found");
            }
            
        }
        else{
            Socket succSocket = new Socket(bs.successorip, bs.successorport);
            DataInputStream istreamsc = new DataInputStream(succSocket.getInputStream());
            DataOutputStream ostreamsc = new DataOutputStream(succSocket.getOutputStream());
            ostreamsc.writeUTF("lookup");
            ostreamsc.writeUTF(Integer.toString(lookKey));
            String keyIs = "";
            keyIs = istreamsc.readUTF();
            if(keyIs.equals("-1")){
                System.out.println("key not found");
            }
            else{
                System.out.println(keyIs);
            }
            System.out.println(istreamsc.readUTF()+"0 ");
        }
    }catch(Exception e){

    }
    }
    public static void insertKey(int key, String value){
    try{
        if(key>bs.predissesorid||bs.predissesorid==0){
            keyValues.put(key, value);
            System.out.println("key successfully added to "+0);
            System.out.println("Visited: "+0);
        }
        else{
            Socket succSocket = new Socket(bs.successorip, bs.successorport);
            DataInputStream istreamsc = new DataInputStream(succSocket.getInputStream());
            DataOutputStream ostreamsc = new DataOutputStream(succSocket.getOutputStream());
            ostreamsc.writeUTF("insert");
            ostreamsc.writeUTF(Integer.toString(key));
            ostreamsc.writeUTF(value);
            System.out.println(istreamsc.readUTF());
            System.out.println(istreamsc.readUTF()+" 0");
        }
    }catch(Exception e){

    }
    }


    public static void delete(int key){
        try{
        System.out.println(bs.predissesorid);
        if(key>bs.predissesorid||bs.predissesorid==0){
            if(keyValues.containsKey(key)){
                keyValues.remove(key);
            System.out.println("successfully removed key from 0");
            System.out.println("visited: 0");
            }
            else{
                System.out.println("key not found");
            }
            
        }
        else{
            Socket succSocket = new Socket(bs.successorip, bs.successorport);
            DataInputStream istreamsc = new DataInputStream(succSocket.getInputStream());
            DataOutputStream ostreamsc = new DataOutputStream(succSocket.getOutputStream());
            ostreamsc.writeUTF("remove");
            ostreamsc.writeUTF(Integer.toString(key));
            System.out.println(istreamsc.readUTF());
            System.out.println(istreamsc.readUTF()+"0");
        }
    }catch(Exception e){

    }
    }
    public static void sendKeyValuesFirst(DataOutputStream out, int id){
        try{
        for(int i = 0; i<=id; i++){
            if(keyValues.containsKey(i)){
                out.writeUTF(Integer.toString(i));
                out.writeUTF(keyValues.get(i));
                keyValues.remove(i);
            }
        }
        out.writeUTF("-1");
    }catch(Exception e){
        System.out.println(e);
    }
    }
    public static void sendKeysFromBoot(DataOutputStream out, int idpred, int id){
        try{
        for(int i = idpred; i<=id; i++){
            if(keyValues.containsKey(i)){
                out.writeUTF(Integer.toString(i));
                out.writeUTF(keyValues.get(i));
                keyValues.remove(i);
            }
        }
        out.writeUTF("-1");
    }catch(Exception e){
        System.out.println(e);
    }
    }

    public static void sendServerInfo(DataInputStream istreamRecieve, DataOutputStream ostreamSend){
      try{ 
        String msgRS = "";
        msgRS = istreamRecieve.readUTF();
        ostreamSend.writeUTF(msgRS);
        msgRS = istreamRecieve.readUTF();
        ostreamSend.writeUTF(msgRS);
        msgRS = istreamRecieve.readUTF();
        ostreamSend.writeUTF(msgRS);
        msgRS = istreamRecieve.readUTF();
        ostreamSend.writeUTF(msgRS);
        msgRS = istreamRecieve.readUTF();
        ostreamSend.writeUTF(msgRS);
        msgRS = istreamRecieve.readUTF();
        ostreamSend.writeUTF(msgRS);
      }catch(Exception e){

      }
    }
    public static String getVisited(ArrayList<Integer> servers, int id){
        Collections.sort(servers);
        String visit = "";
        int x = 0;
        boolean found = false;
        boolean keepgoing = true;
        while(keepgoing){
            if(found == true){
                keepgoing = false;
            }
            visit = visit+servers.get(x)+" ";
            if(servers.get(x)==id){
                found = true;
            }
            x++;
        }
        return visit;
    }

    public static void sendKeyValues(DataInputStream istreamRecieve, DataOutputStream ostreamSend){
    try{
        
        String msgRS = "";
        while(true){

            msgRS = istreamRecieve.readUTF();
            if(msgRS.equals("-1")){
                break;
            }
            ostreamSend.writeUTF(msgRS);
            msgRS = istreamRecieve.readUTF();
            ostreamSend.writeUTF(msgRS);
        }
        ostreamSend.writeUTF("-1");
        }catch(Exception e){

        }
    }
    public static void main(String args[]){
    try{
        InetAddress localHost = InetAddress.getLocalHost();
        String hostBoot = localHost.getHostAddress();
        File config = new File(args[0]);
        Scanner fileScan = new Scanner(config);
        bs.id = fileScan.nextInt();
        bs.listeningPort = fileScan.nextInt();
        serverIDS.add(id);
        ServerSocket bootStrap = new ServerSocket(bs.listeningPort);


        int k = 0;
        String v = "";

        while (fileScan.hasNextLine()==true) {
            k = fileScan.nextInt();
            v = fileScan.next();
            keyValues.put(k, v);
        }
        BootStrapActive bsthread = new BootStrapActive();
        bsthread.start();
        String msg = "";
        
        while(true){
            socket = bootStrap.accept();
            System.out.println("nameServer connected");
            DataInputStream istream = new DataInputStream(socket.getInputStream());
            DataOutputStream ostream = new DataOutputStream(socket.getOutputStream());
            msg = istream.readUTF();
            if(msg.equals("enter")){

                //if the bootstrap is the only one in the server case1
                if(bs.predissesorPort==0){
                    msg = istream.readUTF();
                    bs.predissesorid = Integer.parseInt(msg);
                    bs.successorid = Integer.parseInt(msg);
                    msg = istream.readUTF();
                    bs.predissesorPort = Integer.parseInt(msg);
                    bs.successorport = Integer.parseInt(msg);
                    msg = istream.readUTF();
                    bs.predisessorip = msg;
                    bs.successorip = msg;
                    sendKeyValuesFirst(ostream, bs.predissesorid);
                    ostream.writeUTF(Integer.toString(bs.id));
                    ostream.writeUTF(Integer.toString(bs.listeningPort));
                    ostream.writeUTF(hostBoot);
                    ostream.writeUTF(Integer.toString(bs.id));
                    ostream.writeUTF(Integer.toString(bs.listeningPort));
                    ostream.writeUTF(hostBoot);
                    serverIDS.add(bs.predissesorid);
                    System.out.println(bs.predissesorid+" entered succesfully, visited :"+0);
                }
                //there is at least one server in the system
                else{
                    ServerInfo tempS = new ServerInfo();
                    msg = istream.readUTF();
                    tempS.id = Integer.parseInt(msg);
                    serverIDS.add(tempS.id);
                    //the predissesor IP is smaller than the ID of the nameserver.
                    //keys from the bootstrap will be added into the new nameserver.
                    if(bs.predissesorid>Integer.parseInt(msg)){
                        Socket sucSocket = new Socket(bs.successorip, bs.successorport);
                        DataInputStream istreams = new DataInputStream(sucSocket.getInputStream());
                        DataOutputStream ostreams = new DataOutputStream(sucSocket.getOutputStream());
                        System.out.println(tempS.id+" entered succesfully, visited :"+getVisited(serverIDS, tempS.id));
                        ostreams.writeUTF("enter");
                        ostreams.writeUTF(msg);
                        //send id
                        msg = istream.readUTF();
                        tempS.listeningPort = Integer.parseInt(msg);
                        //port
                        ostreams.writeUTF(Integer.toString(tempS.listeningPort));
                        //send port
                        msg = istream.readUTF();
                        System.out.println(msg);
                        tempS.ip = msg;

                        ostreams.writeUTF(tempS.ip);
                        sendKeyValues(istreams, ostream);
                        sendServerInfo(istreams, ostream);
                        msg = istreams.readUTF();
                        if(msg.equals("update your successor")){
                            bs.successorid = tempS.id;
                            bs.successorport = tempS.listeningPort;
                            bs.successorip = tempS.ip;
                        }
                        
                    }
                    else{
                        tempS.listeningPort = Integer.parseInt(istream.readUTF());
                        tempS.ip = istream.readUTF();
                        sendKeysFromBoot(ostream, bs.predissesorid, tempS.id);
                        Socket predSocket = new Socket(bs.predisessorip, bs.predissesorPort);
                        DataInputStream istreamp = new DataInputStream(predSocket.getInputStream());
                        DataOutputStream ostreamp = new DataOutputStream(predSocket.getOutputStream());
                        ostreamp.writeUTF("update info");
                        ostreamp.writeUTF(Integer.toString(tempS.id));
                        ostreamp.writeUTF(Integer.toString(tempS.listeningPort));
                        ostreamp.writeUTF(tempS.ip);
                        ostream.writeUTF(Integer.toString(bs.predissesorid));
                        ostream.writeUTF(Integer.toString(bs.predissesorPort));
                        ostream.writeUTF(bs.ip);
                        ostream.writeUTF(Integer.toString(bs.id));
                        ostream.writeUTF(Integer.toString(bs.listeningPort));
                        ostream.writeUTF(bs.ip);
                        System.out.println(tempS.id+" entered succesfully, visited :"+0+" "+bs.predissesorid);
                        bs.predissesorid = tempS.id;
                        bs.predissesorPort = tempS.listeningPort;
                        bs.predisessorip = tempS.ip;

                    }
                }

            }
            if(msg.equals("exit")){
                System.out.println("started exit");
                ServerInfo exitingServerInfo = new ServerInfo();
                msg = istream.readUTF();
                exitingServerInfo.id = Integer.parseInt(msg);
                msg = istream.readUTF();
                exitingServerInfo.listeningPort = Integer.parseInt(msg);
                msg = istream.readUTF();
                exitingServerInfo.ip = msg;
                serverIDS.remove(exitingServerInfo.id);

                Socket exitingServerSocket = new Socket(exitingServerInfo.ip, exitingServerInfo.listeningPort);
                DataInputStream istreame = new DataInputStream(exitingServerSocket.getInputStream());
                DataOutputStream ostreame = new DataOutputStream(exitingServerSocket.getOutputStream());
                if(exitingServerInfo.id==bs.successorid&&bs.successorid!=bs.predissesorid){
                    ostreame.writeUTF("exit1");
                    msg = istreame.readUTF();
                    bs.successorid = Integer.parseInt(msg);
                    msg = istreame.readUTF();
                    bs.successorport = Integer.parseInt(msg);
                    msg = istreame.readUTF();
                    bs.successorip = msg;
                    msg = istreame.readUTF();
                    System.out.println(msg);
                }
                else if(exitingServerInfo.id==bs.predissesorid){
                    System.out.println("bootstrap pred");
                    ostreame.writeUTF("exit2");
                    msg = istreame.readUTF();
                    bs.predissesorid = Integer.parseInt(msg);
                    msg = istreame.readUTF();
                    bs.predissesorPort = Integer.parseInt(msg);
                    msg = istreame.readUTF();
                    bs.predisessorip = msg;
                    System.out.println("before boot");
                    addToBootStrap(istreame);
                    System.out.println("after boot");
                    msg = istreame.readUTF();
                    System.out.println(msg);
                    System.out.println("lemon 434 = "+keyValues.get(434));
                    System.out.println("beetroot 325 = "+keyValues.get(325));
                    System.out.println("cherry 288 = "+keyValues.get(288));
                }
                else{
                    ostreame.writeUTF("exit3");
                    msg = istreame.readUTF();
                    System.out.println(msg);
                }

            }


        }
        }
        catch(Exception e){
            System.out.println(e);
        }
    }





}