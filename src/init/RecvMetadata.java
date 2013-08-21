package init;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ConcurrentHashMap;
import java.util.List;
import index.GlobalNamespace;
import index.UploaderMeta;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import test.DebugTool;

public class RecvMetadata implements Runnable {
    public RecvMetadata(String serveraddr, int p, UploaderMeta uploaderMeta){
        listenPort = p;
        pool = Executors.newCachedThreadPool();
        upMeta = uploaderMeta;
    }
    
    public void run() {
        try {
            serverSoc = new ServerSocket(listenPort);
            while (!exit) {
                System.out.println("Waiting a new connection...");
                Socket soc = serverSoc.accept();
                //System.out.println("connection begin...");
                Thread thread = new Thread(new ProcessThread(soc));
                thread.setDaemon(true);
                thread.start();
                //Callable<ConcurrentHashMap<String, GlobalNamespace>> callable = new ProcessThread(soc);
                System.out.println("Connection established...");
                //results.add(pool.submit(callable));
                //future = pool.submit(callable);
                //updateFlag = true;                
            }
            
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                serverSoc.close();
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }
    
    public boolean getUpdateFlag() {
        return updateFlag;
    }
    
    /*public ConcurrentHashMap<String, GlobalNamespace> getGGroup() {
        try {
            if (updateFlag) {
                return (ConcurrentHashMap<String, GlobalNamespace>)future.get();
            }
            else
                return null;
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (ExecutionException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return null;
    }*/
    
    private class ProcessThread implements Runnable {
        ProcessThread(Socket soc) {
            procSoc = soc;                     
        }
        
        @SuppressWarnings("unchecked")
        public void run() {
            try {
                DataOutputStream out = new DataOutputStream(procSoc.getOutputStream());
                ObjectInputStream in = new ObjectInputStream(procSoc.getInputStream());
                ConcurrentHashMap<String, GlobalNamespace> groupGns; 
                //        = (ConcurrentHashMap<String, GlobalNamespace>)in.readObject();
                int count = 0;
                
                while ((groupGns = (ConcurrentHashMap<String, GlobalNamespace>)in.readObject()) != null) {
                    //groupGns = (ConcurrentHashMap<String, GlobalNamespace>)groupGns;
                    //count++;
                    //System.out.println("Count = " + count);
                    //DebugTool.PrintGgns("server recv " + String.valueOf(count), groupGns);                  
                    
                    //System.out.println("newKey: " + newKey);
                    out.writeInt((count++) % UploaderMeta.packetAllowedNum);
                    out.flush();                            
                    //groupGns.remove(newKey);
                                      
                    upMeta.updateGns(groupGns);
                }
                                              
                //return groupGns;
            } catch (IOException e) {
                System.out.println("one socket broken...");
            } catch (ClassNotFoundException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            //return null;
            
        }
        
        private Socket procSoc;
    }
    
    private int listenPort;
    public static volatile boolean exit = false;
    ExecutorService pool;
    UploaderMeta upMeta;
    List<Future<ConcurrentHashMap<String, GlobalNamespace>>> results;
    private boolean updateFlag = false;
    ServerSocket serverSoc;

}
