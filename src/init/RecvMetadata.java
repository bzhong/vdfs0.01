package init;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.LinkedHashMap;
import index.GlobalNamespace;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class RecvMetadata implements Runnable {
    public RecvMetadata(String serveraddr, int p){
        listenPort = p;
        pool = Executors.newFixedThreadPool(2);
    }
    
    public void run() {
        try {
            serverSoc = new ServerSocket(listenPort);
            while (!exit) {
                Socket soc = serverSoc.accept();
                Callable<LinkedHashMap<String, Set<GlobalNamespace>>> callable = new ProcessThread(soc);
                future = pool.submit(callable);
                updateFlag = true;
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
    
    public LinkedHashMap<String, Set<GlobalNamespace>> getGGroup() {
        try {
            return (LinkedHashMap<String, Set<GlobalNamespace>>)future.get();
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (ExecutionException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return null;
    }
    
    private class ProcessThread implements Callable<LinkedHashMap<String, Set<GlobalNamespace>>> {
        ProcessThread(Socket soc) {
            procSoc = soc;                     
        }
        
        public LinkedHashMap<String, Set<GlobalNamespace>> call() {
            try {
                ObjectInputStream in = new ObjectInputStream(procSoc.getInputStream());
                @SuppressWarnings("unchecked")
                LinkedHashMap<String, Set<GlobalNamespace>> groupGns 
                        = (LinkedHashMap<String, Set<GlobalNamespace>>)in.readObject();
                return groupGns;
            } catch (IOException e) {
                e.printStackTrace();
            } catch (ClassNotFoundException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            return null;
            
        }
        
        private Socket procSoc;
    }
    
    private int listenPort;
    public static volatile boolean exit = false;
    ExecutorService pool;
    Future<LinkedHashMap<String, Set<GlobalNamespace>>> future;
    private boolean updateFlag = false;
    ServerSocket serverSoc;

}
