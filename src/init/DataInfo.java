package init;

import index.GlobalFile;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.io.FileOutputStream;

public class DataInfo implements Serializable{
    public boolean storeData(GlobalFile file) {
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
    }
    public GlobalFile extractData() {
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
    }
}
