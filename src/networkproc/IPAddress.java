package networkproc;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.Enumeration;

public class IPAddress {
    public static String getAddr() {
        try {
            Enumeration<NetworkInterface> ifceList = NetworkInterface.getNetworkInterfaces();
            if (ifceList == null) {
                System.out.println("Error: no such network interface...");
            }
            else {
                while (ifceList.hasMoreElements()) {
                    NetworkInterface ifce = ifceList.nextElement(); 
                    //if (ifce.getName().equals("eth0")) {                    
                        Enumeration<InetAddress> addrList = ifce.getInetAddresses();
                        while (addrList.hasMoreElements()) {
                            InetAddress address = addrList.nextElement();
                            if (address.getHostAddress().indexOf(":") == -1 &&
                                    !address.getHostAddress().equals("127.0.0.1")) {
                            //System.out.println(address.getHostAddress());
                            return address.getHostAddress();
                            }
                        }
                    //}
                }
            }
        } catch (SocketException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return null;
    }
}
