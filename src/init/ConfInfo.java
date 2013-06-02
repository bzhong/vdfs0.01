package init;

import java.io.*;
import java.util.prefs.Preferences;
import org.ini4j.Ini;
import org.ini4j.InvalidFileFormatException;

public class ConfInfo {
    String confDir; //directory where conf files lie
    String dataDir; //directory where metadata and data lie
    
    public ConfInfo () {
        
    }
    
    public String findDataDir() {
        if (confDir == null) {
            System.out.println("Init error: undefined configuration directory!");
            System.exit(1);
        }
        String targetDir = new String();
        //find a specified file in a directory
        //find a specified conf line in a ini file e.g. dataDir=/home/brady
        return targetDir;
    }
    
    public static void main (String[] args) {
        ConfInfo cInfo = new ConfInfo();
        try {
            Ini ini = new Ini(new File("basicConf.ini"));
            cInfo.confDir = ini.get("Directory").fetch("dir");
            
        } catch (InvalidFileFormatException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        
    }
}
