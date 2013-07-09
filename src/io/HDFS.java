package io;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;

import index.GlobalNamespace;
import index.UploaderMeta;

import java.io.*;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;


public class HDFS {
    public HDFS() {
        /*try {
            //fs = FileSystem.get(new Configuration());
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }*/
    }
    
    /*public boolean close() {
        try {
            fs.close();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return true;
    }*/

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
            br.close();
            in.close();
            fs.close();
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
            fs.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public boolean delete(Path delFile) {
        try {
            FileSystem fs = FileSystem.get(new Configuration());
            fs.delete(delFile, false);
            fs.close();
        } catch (Exception e) {

        }
        return true;
    }

    public boolean displayNS(GlobalNamespace gns){
        Iterator<String> iter = gns.getGNS().iterator();
        while (iter.hasNext()) {
            System.out.println(iter.next());
        }
        return true;
    }
    
    public boolean displayGroupNS(UploaderMeta uploaderMeta) {
        Iterator<Entry<String, GlobalNamespace>> iter = uploaderMeta.getGroupGns().entrySet().iterator();
        while (iter.hasNext()) {
            Map.Entry<String, GlobalNamespace> pairs = (Map.Entry<String, GlobalNamespace>)iter.next();
            System.out.println(pairs.getKey() + ": ");
            Iterator<String> subIter = pairs.getValue().getGNS().iterator();
            while (subIter.hasNext()) {             
                    System.out.println(subIter.next());                
            }
            
        }
        return true;
    }

    //private FileSystem fs;

    /*public static void main(String[] args) throws IOException {
    String myString = new String();
    ReadFile myRead = new ReadFile();
    myString = myRead.readfunc(new Path(args[0]));
    System.out.print(myString);
    System.out.print("");
  }*/
}
