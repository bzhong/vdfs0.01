package init;

import index.ConfigurableGlobalView;
import index.GlobalNamespace;
import index.UploaderMeta;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import test.DebugTool;
import java.util.concurrent.ConcurrentHashMap;
//import test.DistTimeStat;
import test.HDFSClient;

public class SendMetadata implements Runnable {
    public SendMetadata(String serveraddr, int p, String ownership, 
            ConcurrentHashMap<String, GlobalNamespace> gnsGroup, GlobalNamespace gns){
        serverAddress = ownership;
        port = p;
        startTime = 0;
        endTime = 0;
        gnsG = gnsGroup;
        localAddr = serveraddr;
        localGns = gns;
        packetNum = 0;
    }
    
    /*public boolean getGroupGns(ConcurrentHashMap<String, GlobalNamespace> gnsgroup) {
        gnsG = gnsgroup;
        return true;
    }*/
    
    public boolean setGroupGns(ConcurrentHashMap<String, GlobalNamespace> gnsgroup) {
        gnsG = gnsgroup;
        return true;
    }
    
    public int getPacketNum() {
        return packetNum;
    }
    
    public boolean timePeriod (long start, long end) {
        //upload vdfs.db to upper data center about every 60s
        if ((end - start) >> 14 > 0) {
            Thread statThread = new Thread(new DistTimeStat());
            statThread.setDaemon(true);
            statThread.start();
            return true;
        }
        else {
            return false;
        }
    }
    
    public void run() {
        while (!exit) {
            try {
                //System.out.println("build socket...");
                Socket soc = new Socket(serverAddress, port);
                soc.setSoTimeout(10000);

                System.out.println("socket built...");
                // in = new BufferedReader(new InputStreamReader(soc.getInputStream()));
                out = new ObjectOutputStream(soc.getOutputStream());
                in = new DataInputStream(soc.getInputStream());
                startTime = System.currentTimeMillis();
                //System.out.println("begin loop...");
                long count = 0;
                while (!exit) {
                    endTime = System.currentTimeMillis();
                    if (timePeriod(startTime, endTime)) {
                        //System.out.println("time up : " 
                        //    +  String.valueOf((endTime - startTime) / 1000) 
                            //+ " send: " + String.valueOf(packetNum)  
                            //+ " curDistSuccNum: " 
                            //+ String.valueOf(UploaderMeta.currentDistSuccNum)
                        //    );
                        if (gnsG.containsKey(localAddr)) {
                            gnsG.remove(localAddr);
                        }
                        gnsG.put(localAddr, localGns);
                        gnsG = ConfigurableGlobalView.restoreFromGV(gnsG, HDFSClient.level, HDFSClient.specialDir);
                        //gnsG.put(String.valueOf(packetNum), null);
                        //synchronized (gnsG) {
                            out.writeObject(gnsG);
                            out.flush();
                            out.reset();
                        //}
                        //gnsG.remove(packetNum);
                        int packetAck = in.readInt();
                        //System.out.println("packetAck: " + String.valueOf(packetAck));
                        //if (packetAck == packetNum) {
                            //System.out.println("Hit!! packet num: " + String.valueOf(packetAck));
                            int value = UploaderMeta.currentDistSuccNum.get(packetAck);
                            UploaderMeta.currentDistSuccNum.set(packetAck, ++value);
                            //packetNum = (packetNum + 1) % UploaderMeta.packetAllowedNum;
                        //}
                        count++;
                        //DebugTool.PrintGgns("client send" + String.valueOf(count), gnsG);

                        //System.out.println("client upload over...");
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
                //System.out.println("no existed server, sleep and try again...");
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e1) {
                    // TODO Auto-generated catch block
                    e1.printStackTrace();
                }
            }
        }
    }
    
    public static class DistTimeStat implements Runnable {
        public DistTimeStat() {
            UploaderMeta.startTransferTime.set(packetNum, System.currentTimeMillis());
            //startExecTime = System.currentTimeMillis();
        }
        public void run() {
            while (!exit) {
                for (int count = 0; count < UploaderMeta.packetAllowedNum; count++) {
                    if (UploaderMeta.currentDistSuccNum.get(count) == HDFSClient.numOfNodes) {
                        endExecTime = System.currentTimeMillis();
                        System.out.println("Total time is: " 
                                + String.valueOf((endExecTime - UploaderMeta.startTransferTime.get(count)) 
                                        + "ms for packet " + String.valueOf(count)));
                        UploaderMeta.currentDistSuccNum.set(count, 0);
                        packetNum = (packetNum + 1) % UploaderMeta.packetAllowedNum;
                        //startExecTime = System.currentTimeMillis();
                        return;
                    } else {
                        try {
                            Thread.sleep(10);
                        } catch (InterruptedException e) {
                            // TODO Auto-generated catch block
                            e.printStackTrace();
                        }
                    }
                } 
            }
        }
        private long startExecTime;
        private long endExecTime;
        public static boolean exit = false;
    }
    
    private String serverAddress;
    private String localAddr;
    private int port;
    private ObjectOutputStream out;
    private DataInputStream in;
    public static volatile boolean exit = false;
    private long startTime;
    private long endTime;
    private GlobalNamespace localGns;
    private ConcurrentHashMap<String, GlobalNamespace> gnsG;
    private static int packetNum;
}
