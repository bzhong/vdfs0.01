package init;

import index.GlobalFile;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.io.FileOutputStream;
import java.util.HashSet;
import java.util.Set;

public class MetadataStore implements Serializable{
    public static boolean storeData(Set<String> globalns, String filename) {
        try {
            ObjectOutputStream objout = new ObjectOutputStream(new FileOutputStream(filename));
            objout.writeObject(globalns);
            objout.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return true;
    }
    
    public static Set<String> extractData(String filename) {
        Set<String> gns = new HashSet<String>();
        try {
            File dbFile = new File(filename);
            //if (!dbFile.exists()) {
            dbFile.createNewFile();
            //}
            ObjectInputStream objin = new ObjectInputStream(new FileInputStream(filename));
            gns = (HashSet<String>)objin.readObject();
            objin.close();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        return gns;
    }
    
    /*public boolean storeData(GlobalFile file) {
        ConfInfo cinfo = new ConfInfo();
        
        String curDataDir = cinfo.findDataDir();
        if (!curDataDir.endsWith("/"))
            curDataDir = curDataDir + "/";
        try {
            FileOutputStream foutstream = 
                    new FileOutputStream(curDataDir + "fileobj.txt");
            ObjectOutputStream objout = 
                    new ObjectOutputStream(foutstream);
            objout.writeObject(file);
            objout.close();
            foutstream.close();
        } catch (FileNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return true;
    }*/
    /*public GlobalFile extractData() {
        ConfInfo cinfo = new ConfInfo();
        String curDataDir = cinfo.findDataDir();
        GlobalFile gfile = null;
        if (!curDataDir.endsWith("/"))
            curDataDir = curDataDir + "/";
        try {
            FileInputStream fin = new FileInputStream(curDataDir + "fileobj.txt");
            ObjectInputStream objin = new ObjectInputStream(fin);
            gfile = (GlobalFile) objin.readObject();
            objin.close();
            fin.close();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return gfile;
    }*/
}
