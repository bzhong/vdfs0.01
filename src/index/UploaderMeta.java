package index;


import init.MetadataStore;
import init.RecvMetadata;
import init.SendMetadata;

import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Vector;

import test.HDFSClient;

import networkproc.IPAddress;

//import com.google.common.collect.HashMultimap;

public class UploaderMeta {
    public UploaderMeta(String tDir, String ipAddr, int numOfNodes) {
        topDir = tDir;
        gnsGroup = MetadataStore.extractGroupMeta(topDir, groupMetaFilePath);
        clusterNum = numOfNodes;
        currentDistSuccNum = new ArrayList<Integer>(packetAllowedNum);
        for (int count = 0; count < packetAllowedNum; count++)
            currentDistSuccNum.add(0);
        startTransferTime = new ArrayList<Long>(packetAllowedNum);
        for (int count = 0; count < packetAllowedNum; count++)
            startTransferTime.add(0L);
        
    }
    
    public void flushToDisk() {
        MetadataStore.storeGroupMeta(gnsGroup, topDir, groupMetaFilePath);
    }
    
    public ConcurrentHashMap<String, GlobalNamespace> getGroupGns() {
        
        return gnsGroup;
    }
    
    public static ArrayList<Integer> getClusterNum() {
        return currentDistSuccNum;
    }
    
    public boolean updateGns(ConcurrentHashMap<String, GlobalNamespace> ggroup) {
        long totalTime = 0;
        if (ggroup != null) {
            
            for (String newKey : ggroup.keySet()) {
                if (gnsGroup.containsKey(newKey)) {
                    gnsGroup.remove(newKey);
                    
                }
                
                //generating configured global view
                
                
                //System.out.println("preparetransform " + newKey);
                long startGV = System.currentTimeMillis();
                GlobalNamespace tmpGns = ConfigurableGlobalView.transformToConfGV(
                        newKey, ggroup.get(newKey), HDFSClient.level, 
                        HDFSClient.specialDir);
                long endGV = System.currentTimeMillis();
                totalTime += endGV - startGV;
                gnsGroup.put(newKey, tmpGns);
                
            }
            
            //System.out.println("GVTrans Time is " + totalTime);
        }
        return true;
    }
    
    public boolean addGnsToGroupgns(GlobalNamespace localGns) {
        String ipAddr = IPAddress.getAddr();
        if (gnsGroup.containsKey(ipAddr)) {
            gnsGroup.remove(ipAddr);
        }
        gnsGroup.put(ipAddr, localGns);
        return true;
        
    }
    
    public boolean setGroupgns(ConcurrentHashMap<String, GlobalNamespace> ggns) {
        gnsGroup = ggns;
        return true;
    }
    
    public boolean uploadMeta(String ipAddr, int port, String ownerAddr, GlobalNamespace gns) {
        sendMetadata = new SendMetadata(ipAddr, port, ownerAddr, gnsGroup, gns);
        Thread sendMeta = new Thread(sendMetadata);
        sendMeta.setDaemon(true);
        sendMeta.start();
        //sendMetadata.run();
        return true;
    }
    
    public boolean recvMeta(String serverAddr, int port) {
        RecvMetadata recvMetadata = new RecvMetadata(serverAddr, port, this);
        Thread recvMeta = new Thread(recvMetadata);
        recvMeta.setDaemon(true);
        recvMeta.start();
        //recvMetadata.run();
        //while (!recvMetadata.getUpdateFlag()) {
        //    ;
        //}
        return true;
        
    }
    
    
    private ConcurrentHashMap<String, GlobalNamespace> gnsGroup;
    private String groupMetaFilePath = "groupvdfs.db";
    private String topDir;
    SendMetadata sendMetadata;
    private int clusterNum;
    public static ArrayList<Integer> currentDistSuccNum;
    public static ArrayList<Long> startTransferTime;
    public static final int packetAllowedNum = 10;
}
