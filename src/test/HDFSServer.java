package test;

import index.GlobalNamespace;
import index.UploaderMeta;
import io.HDFS;

import java.util.Scanner;
import networkproc.IPAddress;

import org.apache.hadoop.fs.Path;

public class HDFSServer {
    public HDFSServer() {

    }
    
    protected class UpdateGroupGns implements Runnable {
        public void run() {
            startTime = System.currentTimeMillis();
            while (!exit) {
                endTime = System.currentTimeMillis();
                if (timePeriod(startTime, endTime)) {
                    System.out.println("time up: "
                            + String.valueOf((endTime - startTime) / 1000));
                    uploaderMeta.addGnsToGroupgns(serverGns);
                    // uploaderMeta.uploadMeta(ipAddr, port, ownerAddr,
                    // clntGns);
                    System.out.println("update over...");
                    startTime = System.currentTimeMillis();
                    // break;

                }
            }

        }

        public boolean timePeriod(long start, long end) {
            // upload vdfs.db to upper data center about every 60s
            if ((end - start) >> 14 > 0) {
                return true;
            } else {
                return false;
            }
        }

        private long startTime;
        private long endTime;
        public static final boolean exit = false;
    }

    public static void main(String[] args) {
        HDFSServer hdfsServer = new HDFSServer();
        hdfsServer.serverGns = new GlobalNamespace(topDir);
        String ipAddr = IPAddress.getAddr();
        System.out.println("local IP: " + ipAddr);
        hdfsServer.uploaderMeta = new UploaderMeta(topDir, hdfsServer.ipAddr);
        Thread updateThread = new Thread(hdfsServer.new UpdateGroupGns());
        updateThread.setDaemon(true);
        updateThread.start();
        //UploaderMeta uploadMeta = new UploaderMeta(topDir, ipAddr);
        hdfsServer.uploaderMeta.recvMeta(ipAddr, port);
        // uploadMeta.updateGns(gGNS);

        System.out.println("Begin Test...");
        HDFS fileSystem = new HDFS();
        System.out.println("construction ok");
        Scanner scanner = new Scanner(System.in);
        String newFile;
        char operation;
        while (!((newFile = scanner.next()).equals("-1"))) {
            System.out.println("newFile: " + newFile);

            if (newFile.equals("ls")) {
                fileSystem.displayNS(hdfsServer.serverGns);
                continue;
            }

            if (newFile.equals("lsg")) {
                fileSystem.displayGroupNS(hdfsServer.uploaderMeta);
                continue;
            }

            // create, write, read and delete a file
            String filename = new String(newFile);
            String absfilepath = "hdfs://" + ipAddr + ":9000/user/hadoop/"
                    + filename;
            String content = "Hello, world!";
            Path path = new Path(absfilepath);

            operation = scanner.next().charAt(0);
            switch (operation) {
            case 'c':// create
                if (hdfsServer.serverGns.findPath(filename)) {
                    System.out.println("Error: existed file " + filename);
                    continue;
                } else {
                    fileSystem.create(path, true);
                    hdfsServer.serverGns.addPath(filename);
                    System.out.println("create ok...");
                }
                break;
            case 'w':// write
                if (!hdfsServer.serverGns.findPath(filename)) {
                    System.out.println("Error: no such file " + filename);
                    continue;
                } else {
                    fileSystem.write(content, path);
                    System.out.println("write ok...");
                }
                break;
            case 'r':// read
                if (!hdfsServer.serverGns.findPath(filename)) {
                    System.out.println("Error: no such file " + filename);
                    continue;
                } else {
                    String result = new String(fileSystem.read(path));
                    System.out.println(result);
                    System.out.println("read ok...");
                }
                break;
            case 'd':// delete
                if (!hdfsServer.serverGns.findPath(filename)) {
                    System.out.println("Error: no such file " + filename);
                    continue;
                } else {
                    hdfsServer.serverGns.removePath(filename);
                    fileSystem.delete(path);
                    System.out.println("delete ok...");
                }
                break;
            }
            hdfsServer.serverGns.flushToDisk();
        }

        System.out.println("ending the session...");
        scanner.close();
        //SendMetadata.exit = true;
        hdfsServer.uploaderMeta.flushToDisk();

    }

    // private static String ipAddr = "192.168.5.49";
    private String ipAddr;
    private static int port = 3456;
    private static String topDir = "vdfsServer";
    private GlobalNamespace serverGns;
    private UploaderMeta uploaderMeta;
}
