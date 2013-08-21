package index;

import java.util.HashSet;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;

import test.HDFSClient;

public class ConfigurableGlobalView {
    public static GlobalNamespace transformToConfGV(String IP, GlobalNamespace gns, int level, String dir) {
        if (level <= 0)
            return gns;
        HashSet<String> tmpGns = gns.getGNS();
        HashSet<String> newGns = new HashSet<String>();
        Iterator<String> iter = tmpGns.iterator();
        boolean modifyFlag = false;
        while (iter.hasNext()) {
            
            String tmpString = iter.next();
            char[] tmpStringChar = tmpString.toCharArray();
            int charLen = tmpStringChar.length;
            int tmpLevel = 0;
            int prevCount = 0;
            for (int count = 0; count < charLen; count++) {
                if (tmpStringChar[count] != '/') {
                    
                    continue;
                }
                if (tmpLevel != level - 1) {
                    tmpLevel++;
                    prevCount = count;
                } 
                else if (tmpLevel == level - 1) {
                    //System.out.println("prevCount: " + String.valueOf(prevCount) + " count: " +String.valueOf(count));
                    //System.out.println("level: " + String.valueOf(HDFSClient.level) + " dir: " + HDFSClient.specialDir);                    
                    
                    if (tmpLevel == 0 && tmpString.substring(prevCount, count).equals(dir)) {
                        String newStr = tmpString.substring(prevCount, count + 1)
                                + IP + tmpString.substring(count);
                        newGns.add(newStr);
                        modifyFlag = true;
                        
                    }
                    else if (tmpLevel != 0 && tmpString.substring(prevCount + 1, count).equals(dir)){
                        String newStr = tmpString.substring(prevCount + 1, count + 1)
                                + IP + "/" + tmpString.substring(0, prevCount)
                                + tmpString.substring(count);
                        newGns.add(newStr);
                        modifyFlag = true;
                        
                    }
                    break; 
                }
            }
            
            if (modifyFlag != true) {
                newGns.add(tmpString);
                modifyFlag = false;
            }
            
        }
        
        //Iterator<String> testIter = newGns.iterator();
        //while (testIter.hasNext()) {
        //    System.out.println(testIter.next());
        //}
        
        return new GlobalNamespace(newGns);
    }
    
    //public static String restoreToUnifiedPath(String path) {
        
    //}
    
    public static ConcurrentHashMap<String, GlobalNamespace> restoreFromGV(
            ConcurrentHashMap<String, GlobalNamespace> ggns, int level, String dir) {
        if (level <= 0)
            return ggns;
        boolean modifyFlag = false;
        ConcurrentHashMap<String, GlobalNamespace> newGgns = new ConcurrentHashMap<String, GlobalNamespace>();
        for (String IP : ggns.keySet()) {
            GlobalNamespace tmpGns = ggns.get(IP);
            HashSet<String> newGns = new HashSet<String>();
            Iterator<String> iter = tmpGns.getGNS().iterator();
            while (iter.hasNext()) {
                
                String tmpString = iter.next();
                char[] tmpStringChar = tmpString.toCharArray();
                int charLen = tmpStringChar.length;
                int tmpLevel = 0;
                int prevCount = 0;
                int origPathHead = 0;
                for (int count = 0; count < charLen; count++) {
                    if (tmpStringChar[count] != '/') {
                        
                        continue;
                    }
                    
                    if (tmpLevel == 0) {
                        origPathHead = count;
                    }
                    
                    if (tmpLevel == 1 && tmpString.substring(prevCount, count).equals(IP)) {
                        prevCount = count;
                        if (level == 1) {
                            String newStr = tmpString.substring(0, origPathHead) + tmpString.substring(count);
                            newGns.add(newStr);
                            break;
                        }
                    }
                    else
                        break;
                    
                    if (tmpLevel != level) {
                        tmpLevel++;
                        prevCount = count;
                    } 
                    else if (tmpLevel == level) {
                        //System.out.println("prevCount: " + String.valueOf(prevCount) + " count: " +String.valueOf(count));
                        //System.out.println("level: " + String.valueOf(HDFSClient.level) + " dir: " + HDFSClient.specialDir);
                        
                        String newStr = tmpString.substring(prevCount + 1, count + 1) 
                                + tmpString.substring(0, origPathHead) 
                                + tmpString.substring(count);
                        newGns.add(newStr);
                        modifyFlag = true;
                    }
                }                
                if (modifyFlag != true) {
                    newGns.add(tmpString);
                    modifyFlag = false;
                }
                
            }
            
            newGgns.put(IP, new GlobalNamespace(newGns));
        }
        
        return newGgns;
    }
}
