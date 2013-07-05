package test;

import java.util.Scanner;

import networkproc.IPAddress;

import org.apache.hadoop.fs.Path;

import index.GlobalNamespace;
import index.UploaderMeta;
import init.SendMetadata;
import io.HDFS;

public class Client {
    Client() {
        
    }
    
    public static void  main(String[] args) {
        GlobalNamespace clntGns = new GlobalNamespace();        
        String ipAddr = IPAddress.getAddr();
        System.out.println("local IP: " + ipAddr);
        UploaderMeta uploadMeta = new UploaderMeta(ipAddr);
        uploadMeta.uploadMeta(ipAddr, port, ownerAddr);
        
        System.out.println("Begin Test...");
        HDFS fileSystem = new HDFS();
        System.out.println("construction ok");
        Scanner scanner = new Scanner(System.in);
        String newFile;
        while (!((newFile = scanner.next()).equals("-1"))) {
            System.out.println("newFile: " + newFile);
          //create, write, read and delete a file
            String filename = new String(newFile);
            String absfilepath = "hdfs://" + ipAddr + ":9000/user/hadoop/" + filename;
            String content = "Hello, world!";
            Path path = new Path(absfilepath);
            
            if (clntGns.findPath(filename)) {
                System.out.println("Error: existed file " + filename);
                continue;
            } 
            else {          
                fileSystem.create(path, true);
                clntGns.addPath(filename);
                System.out.println("create ok...");
            }       
            
            if (!clntGns.findPath(filename)) {
                System.out.println("Error: no such file " + filename);
                continue;
            }
            else {
                fileSystem.write(content, path);
                System.out.println("write ok...");
            }
            
            if (!clntGns.findPath(filename)) {
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
                return;
            } 
            else {          
                gns.removePath(filename);
                rf.delete(path);
                System.out.println("delete ok...");
            }   */
            
            clntGns.flushToDisk();
        }
        
        scanner.close();
        
        SendMetadata.exit = true;
        
    }
        
    //private static String ipAddr = "192.168.5.115";
    private static int port = 3456;
    private static String ownerAddr = "192.168.5.49";
}
