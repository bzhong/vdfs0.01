package io;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import java.io.*;


public class ReadFile {
  public ReadFile() {
    
  }
  
  public boolean createfunc(Path newfile, boolean overwrite) {
	  try {
		  FileSystem fs = FileSystem.get(new Configuration());
		  fs.create(newfile, overwrite);
	  } catch (Exception e) {
		  
	  }
	  return true;
  }
  
  public String readfunc(Path inFile) {
    String desc = new String();
    try {
      FileSystem fs = FileSystem.get(new Configuration());
      FSDataInputStream in = fs.open(inFile);
      BufferedReader br = new BufferedReader(new InputStreamReader(in));
      String line;
      StringBuilder sb = new StringBuilder();
      while ((line = br.readLine()) != null) {
        sb.append(line);
        sb.append("\n");
      }
      desc = sb.toString();
      in.close();
    } catch (Exception e) {
      System.out.println("Read Error!");
    }
    return desc;
  }
  
  public void writefunc(String str, Path outFile) {
	  try {
		  FileSystem fs = FileSystem.get(new Configuration());
		  FSDataOutputStream out = fs.create(outFile, true);
		  BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(out));
		  bw.write(str);
		  bw.close();
	  } catch (Exception e) {
		  System.out.println("Write Error!");
	  }
  }
  
  public boolean deletefunc(Path delFile) {
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
