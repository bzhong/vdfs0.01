package authorization;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.concurrent.ConcurrentHashMap;

// a white list to allow some IPs to access system

public class AuthorizationList {
    public void defineAuth(String IP, String meta) {
        if (authControl.contains(IP)) {
            authControl.get(IP).add(meta);
        }
        else {
            authControl.put(IP, new HashSet<String>());
            authControl.get(IP).add(meta);
        }            
    }
    public boolean check(String IP, String meta) {
        HashSet<String> arr = authControl.get(IP);
        if (arr.contains(meta))
            return true;
        else
            return false;
    }
    
    public ConcurrentHashMap<String, HashSet<String> > authControl;
        
}
