package index;

import org.apache.hadoop.fs.*;
import java.io.Serializable;

public class GlobalFile {
    int globalNum;
    int storageType;
    public String filePath; 
    
    public GlobalFile (int num, int type, String path) {
        this.globalNum = num;
        this.storageType = type;
        this.filePath = new String(path);
    }
    
    public int getGlobalNum() {
        return globalNum;
    }
    
    public int getStorageType() {
        return storageType;
    }
    
    public Path getFilePath() {
        return new Path(filePath);
    }
}
