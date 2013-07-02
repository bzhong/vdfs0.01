package index;

import init.MetadataStore;

import java.util.HashSet;
import java.util.Set;


public class GlobalNamespace {
    public GlobalNamespace(){
        //globalns = new HashSet<String>();
        metafilepath = new String("vdfs.db");
        globalns = MetadataStore.extractData(metafilepath);
    }
    
    public void flushToDisk() {
        MetadataStore.storeData(globalns, metafilepath);
    }
    
    public boolean addPath(String str) {
        if (str.isEmpty()) {
            System.out.println("Error: invalid file path " + str);
            return false;
        }
        else {
            if (globalns.contains(str)) {
                System.out.println("Error: duplicated file path " + str);
                return false;
            }
            else {
                globalns.add(str);
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
            if (globalns.contains(str)) {
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
            if (!globalns.contains(str)) {
                System.out.println("Error: no such file in " + str);
                return false;
            }
            else {
                globalns.remove(str);
                return true;
            }
        }
    }
    
    private Set<String> globalns;
    private String metafilepath;
}
