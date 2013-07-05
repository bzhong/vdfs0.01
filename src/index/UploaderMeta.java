package index;


import init.MetadataStore;
import init.RecvMetadata;
import init.SendMetadata;

import java.util.LinkedHashMap;
import java.util.Set;

//import com.google.common.collect.HashMultimap;

public class UploaderMeta {
    public UploaderMeta(String ipAddr) {
        gnsGroup = MetadataStore.extractGroupMeta(groupMetaFilePath);
    }
    
    public void flushToDisk() {
        MetadataStore.storeGroupMeta(gnsGroup, groupMetaFilePath);
    }
    
    public boolean updateGns(LinkedHashMap<String, Set<GlobalNamespace>> ggroup) {
        for (String newKey : ggroup.keySet()) {
            if (gnsGroup.containsKey(newKey)) {
                gnsGroup.remove(newKey);
                gnsGroup.put(newKey, ggroup.get(newKey));
            }
        }
        return true;
    }
    
    public boolean uploadMeta(String ipAddr, int port, String ownerAddr) {
        SendMetadata sendMetadata = new SendMetadata(ipAddr, port, ownerAddr);
        sendMetadata.run();
        return true;
    }
    
    public LinkedHashMap<String, Set<GlobalNamespace>> recvMeta(String serverAddr, int port) {
        RecvMetadata recvMetadata = new RecvMetadata(serverAddr, port);
        recvMetadata.run();
        while (!recvMetadata.getUpdateFlag()) {
            ;
        }
        return recvMetadata.getGGroup();
        
    }
    
    
    private LinkedHashMap<String, Set<GlobalNamespace>> gnsGroup;
    private String groupMetaFilePath = "groupvdfs.db";
}
