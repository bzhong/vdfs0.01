package test;

import java.util.Scanner;

import networkproc.IPAddress;

import org.apache.hadoop.fs.Path;

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
                    System.out.println("time up: "
                            + String.valueOf((endTime - startTime) / 1000));
                    uploaderMeta.addGnsToGroupgns(clntGns);
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
        HDFSClient hdfsClient = new HDFSClient();
        hdfsClient.clntGns = new GlobalNamespace(topDir);
        hdfsClient.ipAddr = IPAddress.getAddr();
        System.out.println("local IP: " + hdfsClient.ipAddr);
        ownerAddr = args[0];
        System.out.println("server IP: " + ownerAddr);
        hdfsClient.uploaderMeta = new UploaderMeta(topDir, hdfsClient.ipAddr);
        Thread updateThread = new Thread(hdfsClient.new UpdateGroupGns());
        updateThread.setDaemon(true);
        updateThread.start();
        hdfsClient.uploaderMeta.uploadMeta(hdfsClient.ipAddr, port, ownerAddr,
                hdfsClient.clntGns);

        System.out.println("Begin Test...");
        HDFS fileSystem = new HDFS();
        System.out.println("construction ok");
        Scanner scanner = new Scanner(System.in);
        String newFile;
        char operation;
        while (!((newFile = scanner.next()).equals("-1"))) {
            System.out.println("newFile: " + newFile);

            if (newFile.equals("ls")) {
                fileSystem.displayNS(hdfsClient.clntGns);
                continue;
            }

            if (newFile.equals("lsg")) {
                fileSystem.displayGroupNS(hdfsClient.uploaderMeta);
                continue;
            }
            // create, write, read and delete a file
            String filename = new String(newFile);
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
                if (!hdfsClient.clntGns.findPath(filename)) {
                    System.out.println("Error: no such file " + filename);
                    continue;
                } else {
                    fileSystem.write(content, path);
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

        System.out.println("ending the session...");
        scanner.close();

        SendMetadata.exit = true;
        hdfsClient.uploaderMeta.flushToDisk();

    }

    // private static String ipAddr = "192.168.5.115";
    private String ipAddr;
    private static int port = 3456;
    private static String ownerAddr;
    private static String topDir = "vdfsClient";
    private GlobalNamespace clntGns;
    private UploaderMeta uploaderMeta;
}
