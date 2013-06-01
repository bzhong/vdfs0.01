package init;

public class confInfo {
    String confDir; //directory where conf files lie
    String dataDir; //directory where metadata and data lie
    
    public confInfo (String dir) {
        confDir = dir;
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
        confInfo cInfo = new confInfo(args[0]);
        
    }
}
