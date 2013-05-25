package test;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import java.io.*;
import io.ReadFile;

public class Hdfs {
	public static void main(String[] args) {
		System.out.println("Begin Test...");
		io.ReadFile rf = new io.ReadFile();
		System.out.println("construction ok");
		//create, write, read and delete a file
		String filename = new String("hdfs://localhost:9000/user/hadoop/test");
		String content = new String("Hello, world!");
		Path path = new Path(filename);
		//rf.createfunc(path, false);
		//rf.writefunc(content, path);
		//rf.readfunc(path);
		//rf.deletefunc(path);
		
		return;
	}
}
