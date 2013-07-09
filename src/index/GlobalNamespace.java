package index;

import init.MetadataStore;

import java.io.Serializable;
import java.util.HashSet;


public class GlobalNamespace implements Serializable {
    public GlobalNamespace(String tDir){
        //globalns = new HashSet<String>();
        topDir = tDir;
        globalNS = MetadataStore.extractData(topDir, META_FILE_PATH);
    }
    
    public void flushToDisk() {
        MetadataStore.storeData(globalNS, topDir, META_FILE_PATH);
    }
    
    public HashSet<String> getGNS() {
        return globalNS;
    }
    
    public boolean addPath(String str) {
        if (str.isEmpty()) {
            System.out.println("Error: invalid file path " + str);
            return false;
        }
        else {
            if (globalNS.contains(str)) {
                System.out.println("Error: duplicated file path " + str);
                return false;
            }
            else {
                globalNS.add(str);
                return true;
            }
        }
    }
    
    public boolean findPath(String str) {
        if (str.isEmpty()) {
            System.out.println("Error: invalid file path " + str);
            return false;
        }
        else {
            if (globalNS.contains(str)) {
                return true;
            }
            else
                return false;
        }
    }
    
    public boolean removePath(String str) {
        if (str.isEmpty()) {
            System.out.println("Error: invalid file path " + str);
            return false;
        }
        else {
            if (!globalNS.contains(str)) {
                System.out.println("Error: no such file in " + str);
                return false;
            }
            else {
                globalNS.remove(str);
                return true;
            }
        }
    }
    
    private HashSet<String> globalNS;
    private String META_FILE_PATH = "vdfs.db";
    private String topDir;
    private static final long serialVersionUID = 1L;
}
