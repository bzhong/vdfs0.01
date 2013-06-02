package test;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import java.io.*;
import io.HDFS;

public class InterfaceTest {
    public InterfaceTest() {}
    
	public static void main(String[] args) {
		System.out.println("Begin Test...");
		HDFS rf = new HDFS();
		System.out.println("construction ok");
		//create, write, read and delete a file
		String filename = new String("hdfs://localhost:9000/user/hadoop/test");
		String content = new String("Hello, world!\n");
		Path path = new Path(filename);
		rf.create(path, true);
		System.out.println("create ok...");
		rf.write(content, path);
		System.out.println("write ok...");
		String result = new String(rf.read(path));
		System.out.println(result);
		System.out.println("read ok...");
		rf.delete(path);
		
		return;
	}
}
