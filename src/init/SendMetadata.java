package init;

import index.GlobalNamespace;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.LinkedHashMap;
import test.DebugTool;

public class SendMetadata implements Runnable {
    public SendMetadata(String serveraddr, int p, String ownership, 
            LinkedHashMap<String, GlobalNamespace> gnsGroup, GlobalNamespace gns){
        serverAddress = ownership;
        port = p;
        startTime = 0;
        endTime = 0;
        gnsG = gnsGroup;
        localAddr = serveraddr;
        localGns = gns;
    }
    
    /*public boolean getGroupGns(LinkedHashMap<String, GlobalNamespace> gnsgroup) {
        gnsG = gnsgroup;
        return true;
    }*/
    
    public boolean setGroupGns(LinkedHashMap<String, GlobalNamespace> gnsgroup) {
        gnsG = gnsgroup;
        return true;
    }
    
    public boolean timePeriod (long start, long end) {
        //upload vdfs.db to upper data center about every 60s
        if ((end - start) >> 14 > 0) {
            return true;
        }
        else {
            return false;
        }
    }
    
    public void run() {
        try {
            System.out.println("build socket...");
            Socket soc = new Socket(serverAddress, port);
            
            System.out.println("socket built...");
           // in = new BufferedReader(new InputStreamReader(soc.getInputStream()));
            out = new ObjectOutputStream(soc.getOutputStream());
            startTime = System.currentTimeMillis();
            System.out.println("begin loop...");
            long count = 0;
            while (!exit) {
                endTime = System.currentTimeMillis();
                if (timePeriod(startTime, endTime)) {
                    System.out.println("time up: " +  String.valueOf((endTime - startTime) / 1000));
                    if (gnsG.containsKey(localAddr)) {
                        gnsG.remove(localAddr);
                    }
                    gnsG.put(localAddr, localGns);
                    out.writeObject(gnsG);
                    out.flush();
                    out.reset();
                    
                    count++;
                    DebugTool.PrintGgns("client send" + String.valueOf(count), gnsG);
                    
                    System.out.println("client upload over...");
                    startTime = System.currentTimeMillis();
                    //break;
                }

            }
            
            out.close();
            //in.close();
            soc.close();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        } catch (IOException e) {
            System.out.println("no existed server, cancel uploading...");
        }
    }
    
    private String serverAddress;
    private String localAddr;
    private int port;
    private ObjectOutputStream out;
    public static volatile boolean exit = false;
    private long startTime;
    private long endTime;
    private GlobalNamespace localGns;
    private LinkedHashMap<String, GlobalNamespace> gnsG;
}
