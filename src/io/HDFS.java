package io;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import java.io.*;


public class HDFS {
  public HDFS() {
    
  }
  
  public boolean create(Path newfile, boolean overwrite) {
	  try {
		  FileSystem fs = FileSystem.get(new Configuration());
		  fs.create(newfile, overwrite);
		  fs.close();
	  } catch (Exception e) {
		  
	  }
	  return true;
  }
  
  public String read(Path inFile) {
    String desc = new String();
    try {
      FileSystem fs = FileSystem.get(new Configuration());
      FSDataInputStream in = fs.open(inFile);
      BufferedReader br = new BufferedReader(new InputStreamReader(in));
      String line;
      StringBuilder sb = new StringBuilder();
      while ((line = br.readLine()) != null) {
        sb.append(line);
        //sb.append("\n");
      }
      desc = sb.toString();
      in.close();
    } catch (Exception e) {
      System.out.println("Read Error!");
    }
    return desc;
  }
  
  public void write(String str, Path outFile) {
	  try {
		  FileSystem fs = FileSystem.get(new Configuration());
		  FSDataOutputStream out = fs.append(outFile);
		  System.out.println("FSDataOutputStream create ok...");
		  BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(out));
		  System.out.println("create buffer succeed...");
		  bw.write(str);
		  bw.close();
		  out.close();
	  } catch (Exception e) {
		  e.printStackTrace();
	  }
  }
  
  public boolean delete(Path delFile) {
	  try {
		  FileSystem fs = FileSystem.get(new Configuration());
		  fs.delete(delFile, false);
	  } catch (Exception e) {
		  
	  }
	  return true;
  }
  
  /*public static void main(String[] args) throws IOException {
    String myString = new String();
    ReadFile myRead = new ReadFile();
    myString = myRead.readfunc(new Path(args[0]));
    System.out.print(myString);
    System.out.print("");
  }*/
}
