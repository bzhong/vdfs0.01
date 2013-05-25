package test;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import java.io.*;
import io.ReadFile;

public class Hdfs {
	public static void main(String[] args) {
		System.out.println("Begin Test...");
		ReadFile rf = new ReadFile();
		System.out.println("construction ok");
		//create, write, read and delete a file
		String filename = new String("hdfs://localhost:9000/user/hadoop/test");
		String content = new String("Hello, world!\n");
		Path path = new Path(filename);
		rf.createfunc(path, true);
		System.out.println("create ok...");
		rf.writefunc(content, path);
		System.out.println("write ok...");
		String result = new String(rf.readfunc(path));
		System.out.println(result);
		System.out.println("read ok...");
		rf.deletefunc(path);
		
		return;
	}
}
