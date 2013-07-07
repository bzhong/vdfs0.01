package init;

import index.GlobalNamespace;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.io.FileOutputStream;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Set;

public class MetadataStore implements Serializable{
    public static boolean storeData(Set<String> namespace, String topDir, 
            String filename) {
        try {
            filename = topDir + "/" + filename;
            ObjectOutputStream out = new ObjectOutputStream(
                    new FileOutputStream(filename));
            out.writeObject(namespace);
            out.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return true;
    }
    
    public static boolean storeGroupMeta(LinkedHashMap<String, 
            Set<GlobalNamespace>> gnsGroup, String topDir, String filename) {
        try {
            filename = topDir + "/" + filename;
            ObjectOutputStream out = new ObjectOutputStream(
                    new FileOutputStream(filename));
            out.writeObject(gnsGroup);
            out.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return true;
    }
    
    @SuppressWarnings("unchecked")
    public static HashSet<String> extractData(String topDir, String filename) {
        HashSet<String> gns = new HashSet<String>();        
        try {
            File dbDir = new File(topDir);
            dbDir.mkdirs();
            filename = topDir + "/" + filename;
            File dbFile = new File(filename);
            //if (!dbFile.exists()) {
            dbFile.createNewFile();
            //}
            ObjectInputStream in = new ObjectInputStream(new 
                    FileInputStream(filename));
            gns = (HashSet<String>)in.readObject();
            in.close();
        } catch (IOException e) {
            System.out.println("no existed db file " + filename);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        return gns;
    }
    
    @SuppressWarnings("unchecked")
    public static LinkedHashMap<String, 
            Set<GlobalNamespace>> extractGroupMeta(String topDir, String filename) {
        LinkedHashMap<String, Set<GlobalNamespace>> gnsGroup 
                = new LinkedHashMap<String, Set<GlobalNamespace>>();
        try {
            File groupDBDir = new File(topDir);
            groupDBDir.mkdirs();
            filename = topDir + "/" + filename;
            File groupDBFile = new File(filename);
            //if (!dbFile.exists()) {
            groupDBFile.createNewFile();
            //}
            if (groupDBFile.length() != 0) {
                ObjectInputStream in = new ObjectInputStream(
                        new FileInputStream(groupDBFile));
                if (in.readObject() != null) { 
                    gnsGroup = (LinkedHashMap<String, 
                            Set<GlobalNamespace>>)in.readObject();
                }
                in.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        return gnsGroup;
    }
    
    private static final long serialVersionUID = 1L;
    
}
