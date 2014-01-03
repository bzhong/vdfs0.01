package test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStreamReader;
import java.util.Random;
import java.util.Scanner;

import networkproc.IPAddress;

import org.apache.hadoop.fs.Path;

import authorization.AuthorizationList;

import index.GlobalNamespace;
import index.UploaderMeta;
import init.SendMetadata;
import io.HDFS;

public class HDFSClient {
    HDFSClient() {

    }
    


    protected class UpdateGroupGns implements Runnable {
        public void run() {
            startTime = System.currentTimeMillis();
            while (!exit) {
                endTime = System.currentTimeMillis();
                if (timePeriod(startTime, endTime)) {
                    //System.out.println("time up: "
                    //        + String.valueOf((endTime - startTime) / 1000));
                    uploaderMeta.addGnsToGroupgns(clntGns);
                    // uploaderMeta.uploadMeta(ipAddr, port, ownerAddr,
                    // clntGns);
                    //System.out.println("update over...");
                    startTime = System.currentTimeMillis();
                    // break;

                }
            }

        }

        public boolean timePeriod(long start, long end) {
            // upload vdfs.db to upper data center about every 60s
            if ((end - start) /1000 > 0) {
                //System.out.println("update local gns to ggns...");
                return true;
            } else {
                return false;
            }
        }

        private long startTime;
        private long endTime;
        public boolean exit = false;
    }
    
    public String generateLongDir(int deep) {
        StringBuilder str = new StringBuilder();
        int count = 0;
        while (count != deep) {
            str.append("dir" + String.valueOf(++count) + "/");
        }
        return str.toString();
    }

    public static void main(String[] args) throws FileNotFoundException {
        HDFSClient hdfsClient = new HDFSClient();
        hdfsClient.clntGns = new GlobalNamespace(topDir);
        hdfsClient.ipAddr = IPAddress.getAddr();
        HDFSClient.numOfNodes = args.length;
        //System.out.println("local IP: " + hdfsClient.ipAddr);       
        //System.out.println("server IP: " + ownerAddr);
        hdfsClient.uploaderMeta = new UploaderMeta(topDir, hdfsClient.ipAddr, HDFSClient.numOfNodes);
        Thread updateThread = new Thread(hdfsClient.new UpdateGroupGns());
        updateThread.setDaemon(true);
        updateThread.start();
        
        AuthorizationList authC = new AuthorizationList();
        
        hdfsClient.uploaderMeta.recvMeta(hdfsClient.ipAddr, port);
        for (int num = 0; num < HDFSClient.numOfNodes; num++) {
            ownerAddr = args[num];
            hdfsClient.uploaderMeta.uploadMeta(hdfsClient.ipAddr, port, ownerAddr,
                    hdfsClient.clntGns);
        }
        

        
        System.out.println("Begin Test...");
        HDFS fileSystem = new HDFS();
        //System.out.println("construction ok");
        Scanner scanner = new Scanner(System.in);
        String newFile;
        char operation;
        while (!((newFile = scanner.next()).equals("-1"))) {
            //System.out.println("newFile: " + newFile);
            String filename;
            
            if (newFile.equals("set")) {
                String userLevel = scanner.next();
                level = Integer.parseInt(userLevel);
                String dir = scanner.next();
                specialDir = dir;
                continue;
            }            
            else if (newFile.equals("createmeta")) {
                int deep = scanner.nextInt();
                String prefixDir = hdfsClient.generateLongDir(deep);
                //fileSystem.genMoreFiles(hdfsClient);
                int maxNumOfFiles = scanner.nextInt();
                //int maxNumOfFiles = Integer.parseInt(numOfFiles, 10);
                Random rand = new Random();
                int randNum = rand.nextInt(1000000);
                for (int count = 0; count < maxNumOfFiles; count++) {
                    filename =  prefixDir + String.valueOf(randNum) + "_" + String.valueOf(count);
                    hdfsClient.clntGns.addPath(filename);
                    System.out.println("create " + String.valueOf(count));
                }
                hdfsClient.clntGns.flushToDisk();
                System.out.println("meta create ok...");
                continue;
            }
            else if (newFile.equals("ls")) {
                fileSystem.displayNS(hdfsClient.clntGns);
                continue;
            }
            else if (newFile.equals("lsg")) {
                fileSystem.displayGroupNS(hdfsClient.uploaderMeta);
                continue;
            }
            else if (newFile.equals("createfiles")) {
                int deep = scanner.nextInt();
                String prefixDir = hdfsClient.generateLongDir(deep);
                //String prefixDir = scanner.next();
                //fileSystem.genMoreFiles(hdfsClient);
                int maxNumOfFiles = scanner.nextInt();
                //String numOfFiles = scanner.next();
                //int maxNumOfFiles = Integer.parseInt(numOfFiles, 10);
                Random rand = new Random();
                int randNum = rand.nextInt(1000000);
                String absfilepath = "hdfs://" + hdfsClient.ipAddr
                        + ":9000/user/hadoop/";
                for (int count = 0; count < maxNumOfFiles; count++) {
                    filename =  prefixDir + String.valueOf(randNum) + "_" + String.valueOf(count);
                    Path path = new Path(absfilepath + filename);
                    fileSystem.create(path, true);
                    hdfsClient.clntGns.addPath(filename);
                    System.out.println("create " + String.valueOf(count));
                }
                hdfsClient.clntGns.flushToDisk();
                System.out.println("create ok...");
                continue;
            }
            else if (newFile.equals("normal")) {
             // create, write, read and delete a file
                newFile = scanner.next();
                filename = new String(newFile);
                String absfilepath = "hdfs://" + hdfsClient.ipAddr
                        + ":9000/user/hadoop/" + filename;
                String content = "Hello, world!";
                Path path = new Path(absfilepath);

                operation = scanner.next().charAt(0);
                switch (operation) {
                case 'c':// create
                    if (hdfsClient.clntGns.findPath(filename)) {
                        System.out.println("Error: existed file " + filename);
                        continue;
                    } else {
                        fileSystem.create(path, true);
                        hdfsClient.clntGns.addPath(filename);
                        System.out.println("create ok...");
                    }
                    break;
                case 'w':// write
                    String src = scanner.next();
                    BufferedReader bufRead = new BufferedReader(new InputStreamReader(new FileInputStream(new File(src))));
                    if (!hdfsClient.clntGns.findPath(filename)) {
                        System.out.println("Error: no such file " + filename);
                        continue;
                    } else {
                        fileSystem.write(bufRead, path);
                        System.out.println("write ok...");
                    }
                    break;
                case 'r':// read
                    if (!hdfsClient.clntGns.findPath(filename)) {
                        System.out.println("Error: no such file " + filename);
                        continue;
                    } else {
                        String result = new String(fileSystem.read(path));
                        System.out.println(result);
                        System.out.println("read ok...");
                    }
                    break;
                case 'd':// delete
                    if (!hdfsClient.clntGns.findPath(filename)) {
                        System.out.println("Error: no such file " + filename);
                        continue;
                    } else {
                        hdfsClient.clntGns.removePath(filename);
                        fileSystem.delete(path);
                        System.out.println("delete ok...");
                    }
                    break;
                }
                hdfsClient.clntGns.flushToDisk();
            }
            else
            {
                System.out.println("syntax error! Input again...");
                continue;
            }
            
        }

        System.out.println("ending the session...");
        scanner.close();

        SendMetadata.exit = true;
        //UpdateGroupGns.exit = true;
        hdfsClient.uploaderMeta.flushToDisk();

    }

    // private static String ipAddr = "192.168.5.115";
    public String ipAddr;
    private static int port = 3456;
    private static String ownerAddr;
    private static String topDir = "vdfsClient";
    public GlobalNamespace clntGns;
    private UploaderMeta uploaderMeta;
    public static int numOfNodes;
    //private static final int MaxNumOfFile = 1000;
    public static int level = 0;
    public static String specialDir;
}
