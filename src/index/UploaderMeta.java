package index;


import init.MetadataStore;
import init.RecvMetadata;
import init.SendMetadata;

import java.util.LinkedHashMap;
import networkproc.IPAddress;

//import com.google.common.collect.HashMultimap;

public class UploaderMeta {
    public UploaderMeta(String tDir, String ipAddr) {
        topDir = tDir;
        gnsGroup = MetadataStore.extractGroupMeta(topDir, groupMetaFilePath);
    }
    
    public void flushToDisk() {
        MetadataStore.storeGroupMeta(gnsGroup, topDir, groupMetaFilePath);
    }
    
    public LinkedHashMap<String, GlobalNamespace> getGroupGns() {
        
        return gnsGroup;
    }
    
    public boolean updateGns(LinkedHashMap<String, GlobalNamespace> ggroup) {
        if (ggroup != null) {
            for (String newKey : ggroup.keySet()) {
                if (gnsGroup.containsKey(newKey)) {
                    gnsGroup.remove(newKey);
                    
                }
                gnsGroup.put(newKey, ggroup.get(newKey));
            }
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
    
    public boolean setGroupgns(LinkedHashMap<String, GlobalNamespace> ggns) {
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
    
    
    private LinkedHashMap<String, GlobalNamespace> gnsGroup;
    private String groupMetaFilePath = "groupvdfs.db";
    private String topDir;
    SendMetadata sendMetadata;
}
