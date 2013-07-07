package index;


import init.MetadataStore;
import init.RecvMetadata;
import init.SendMetadata;

import java.util.LinkedHashMap;
import java.util.Set;

//import com.google.common.collect.HashMultimap;

public class UploaderMeta {
    public UploaderMeta(String tDir, String ipAddr) {
        topDir = tDir;
        gnsGroup = MetadataStore.extractGroupMeta(topDir, groupMetaFilePath);
    }
    
    public void flushToDisk() {
        MetadataStore.storeGroupMeta(gnsGroup, topDir, groupMetaFilePath);
    }
    
    public boolean updateGns(LinkedHashMap<String, Set<GlobalNamespace>> ggroup) {
        if (ggroup != null) {
            for (String newKey : ggroup.keySet()) {
                if (gnsGroup.containsKey(newKey)) {
                    gnsGroup.remove(newKey);
                    gnsGroup.put(newKey, ggroup.get(newKey));
                }
            }
        }
        return true;
    }
    
    public boolean uploadMeta(String ipAddr, int port, String ownerAddr) {
        SendMetadata sendMetadata = new SendMetadata(ipAddr, port, ownerAddr);
        Thread sendMeta = new Thread(sendMetadata);
        sendMeta.start();
        //sendMetadata.run();
        return true;
    }
    
    public LinkedHashMap<String, Set<GlobalNamespace>> recvMeta(String serverAddr, int port) {
        RecvMetadata recvMetadata = new RecvMetadata(serverAddr, port);
        Thread recvMeta = new Thread(recvMetadata);
        recvMeta.setDaemon(true);
        recvMeta.start();
        //recvMetadata.run();
        //while (!recvMetadata.getUpdateFlag()) {
        //    ;
        //}
        return recvMetadata.getGGroup();
        
    }
    
    
    private LinkedHashMap<String, Set<GlobalNamespace>> gnsGroup;
    private String groupMetaFilePath = "groupvdfs.db";
    private String topDir;
}
