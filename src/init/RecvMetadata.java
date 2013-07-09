package init;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.LinkedHashMap;
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
                System.out.println("connection begin...");
                Thread thread = new Thread(new ProcessThread(soc));
                thread.setDaemon(true);
                thread.start();
                //Callable<LinkedHashMap<String, GlobalNamespace>> callable = new ProcessThread(soc);
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
    
    /*public LinkedHashMap<String, GlobalNamespace> getGGroup() {
        try {
            if (updateFlag) {
                return (LinkedHashMap<String, GlobalNamespace>)future.get();
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
                ObjectInputStream in = new ObjectInputStream(procSoc.getInputStream());
                LinkedHashMap<String, GlobalNamespace> groupGns; 
                //        = (LinkedHashMap<String, GlobalNamespace>)in.readObject();
                long count = 0;
                
                while ((groupGns = (LinkedHashMap<String, GlobalNamespace>)in.readObject()) != null) {
                    //groupGns = (LinkedHashMap<String, GlobalNamespace>)groupGns;
                    count++;
                    DebugTool.PrintGgns("server recv " + String.valueOf(count), groupGns);
                   
                    upMeta.setGroupgns(groupGns);
                }
                                              
                //return groupGns;
            } catch (IOException e) {
                e.printStackTrace();
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
    List<Future<LinkedHashMap<String, GlobalNamespace>>> results;
    private boolean updateFlag = false;
    ServerSocket serverSoc;

}
