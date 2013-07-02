package test;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import java.io.*;
import io.HDFS;
import index.GlobalNamespace;

public class InterfaceTest {
    public InterfaceTest() {}
    
	public static void main(String[] args) {
	    GlobalNamespace gns = new GlobalNamespace();
	    
	    
		System.out.println("Begin Test...");
		HDFS rf = new HDFS();
		System.out.println("construction ok");
		//create, write, read and delete a file
		String filename = new String(args[0]);
		String absfilepath = "hdfs://localhost:9000/user/hadoop/" + filename;
		String content = new String("Hello, world!\n");
		Path path = new Path(absfilepath);
		
		if (gns.findPath(filename)) {
		    System.out.println("Error: existed file " + filename);
		    return;
		} 
		else {		    
	        rf.create(path, true);
	        gns.addPath(filename);
	        System.out.println("create ok...");
		}		
		
		if (!gns.findPath(filename)) {
		    System.out.println("Error: no such file " + filename);
		    return;
		}
		else {
		    rf.write(content, path);
		    System.out.println("write ok...");
		}
		
		if (!gns.findPath(filename)) {
		    System.out.println("Error: no such file " + filename);
		    return;
		}
		else {
		    String result = new String(rf.read(path));
	        System.out.println(result);
	        System.out.println("read ok...");
		}
		
		/*if (!gns.findPath(filename)) {
            System.out.println("Error: no such file " + filename);
            return;
        } 
        else {          
            gns.removePath(filename);
            rf.delete(path);
            System.out.println("delete ok...");
        }	*/
		
		gns.flushToDisk();
		
		return;
	}
}
