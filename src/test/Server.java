package test;

import index.GlobalNamespace;
import index.UploaderMeta;
import init.SendMetadata;
import io.HDFS;

import java.util.LinkedHashMap;
import java.util.Scanner;
import java.util.Set;

import networkproc.IPAddress;

import org.apache.hadoop.fs.Path;

public class Server {
    public Server() {
        
    }
    
    public static void  main(String[] args) {
        GlobalNamespace serverGns = new GlobalNamespace(topDir);
        String ipAddr = IPAddress.getAddr();
        System.out.println("local IP: " + ipAddr);
        UploaderMeta uploadMeta = new UploaderMeta(topDir, ipAddr);
        LinkedHashMap<String, Set<GlobalNamespace>> gGNS 
                = uploadMeta.recvMeta(ipAddr, port);
        uploadMeta.updateGns(gGNS);
                
        System.out.println("Begin Test...");
        HDFS fileSystem = new HDFS();
        System.out.println("construction ok");
        Scanner scanner = new Scanner(System.in);
        String newFile;
        while (!((newFile = scanner.next()).equals("-1"))) {
          //create, write, read and delete a file
            String filename = new String(newFile);
            String absfilepath = "hdfs://" + ipAddr + ":9000/user/hadoop/" + filename;
            String content = "Hello, world!";
            Path path = new Path(absfilepath);
            
            if (serverGns.findPath(filename)) {
                System.out.println("Error: existed file " + filename);
                continue;
            } 
            else {          
                fileSystem.create(path, true);
                serverGns.addPath(filename);
                System.out.println("create ok...");
            }       
            
            if (!serverGns.findPath(filename)) {
                System.out.println("Error: no such file " + filename);
                continue;
            }
            else {
                fileSystem.write(content, path);
                System.out.println("write ok...");
            }
            
            if (!serverGns.findPath(filename)) {
                System.out.println("Error: no such file " + filename);
                continue;
            }
            else {
                String result = new String(fileSystem.read(path));
                System.out.println(result);
                System.out.println("read ok...");
            }
            
            /*if (!gns.findPath(filename)) {
                System.out.println("Error: no such file " + filename);
                continue;
            } 
            else {          
                gns.removePath(filename);
                rf.delete(path);
                System.out.println("delete ok...");
            }   */
            
            serverGns.flushToDisk();
        }
        
        System.out.println("ending the session...");
        scanner.close();
        SendMetadata.exit = true;
        
    }
    
    
    //private static String ipAddr = "192.168.5.49";
    private static int port = 3456;
    private static String topDir = "vdfsServer";
}
