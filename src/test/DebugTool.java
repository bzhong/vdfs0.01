package test;

import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Map;
import java.util.Map.Entry;

import index.GlobalNamespace;

public class DebugTool {
    public static void PrintGgns(String operation, ConcurrentHashMap<String, GlobalNamespace> ggns) {
        System.out.println(operation + " ok...");
        System.out.println("show ggns: ");
        Iterator<Entry<String, GlobalNamespace>> iter = ggns.entrySet().iterator();
        if (iter.hasNext()) {
            Map.Entry<String, GlobalNamespace> pairs = (Map.Entry<String, GlobalNamespace>)iter.next();
            System.out.println(pairs.getKey() + ": ");
            Iterator<String> subIter = pairs.getValue().getGNS().iterator();
            while (subIter.hasNext()) {             
                    System.out.println(subIter.next());                
            }
        }
    }
}
