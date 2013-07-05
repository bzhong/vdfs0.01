package init;

import index.GlobalNamespace;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.LinkedHashMap;
import java.util.Set;

public class SendMetadata implements Runnable {
    public SendMetadata(String serveraddr, int p, String ownerip){
        serverAddress = serveraddr;
        port = p;
        startTime = 0;
        endTime = 0;
    }
    
    public boolean getGroupGns(LinkedHashMap<String, Set<GlobalNamespace>> gnsgroup) {
        gnsG = gnsgroup;
        return true;
    }
    
    public boolean timePeriod (long start, long end) {
        //upload vdfs.db to upper data center about every 60s
        if ((end - start) >> 16 > 0) {
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
            in = new BufferedReader(new InputStreamReader(soc.getInputStream()));
            out = new ObjectOutputStream(soc.getOutputStream());
            startTime = System.currentTimeMillis();
            
            while (!exit) {
                endTime = System.currentTimeMillis();
                if (timePeriod(startTime, endTime)) {
                      out.writeObject(gnsG);
                      out.flush();
                      break;
                }
                startTime = System.currentTimeMillis();
            }
            
            out.close();
            in.close();
            soc.close();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        } catch (IOException e) {
            System.out.println("no existed server, cancel uploading...");
        }
    }
    
    private String serverAddress;
    private int port;
    private BufferedReader in;
    private ObjectOutputStream out;
    public static volatile boolean exit = false;
    private long startTime;
    private long endTime;
    private LinkedHashMap<String, Set<GlobalNamespace>> gnsG;
}
