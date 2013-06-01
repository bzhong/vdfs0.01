package index;

import org.apache.hadoop.fs.*;

public class File {
    int globalNum;
    int storageType;
    String filepath; 
    
    public File (int num, int type, String path) {
        globalNum = num;
        storageType = type;
        filepath = new String(path);
    }
    
    public int getGlobalNum() {
        return globalNum;
    }
    
    public int getStorageType() {
        return storageType;
    }
    
    public Path getFilePath() {
        return new Path(filepath);
    }
}
