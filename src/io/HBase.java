package io;

import java.io.IOException;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.HBaseConfiguration;

public class HBase {
    public HBase () {}
    
    public boolean createTable(String tableName, String[] families) {
        Configuration conf = HBaseConfiguration.create(new Configuration());
        try {
            HBaseAdmin ha = new HBaseAdmin(conf);
            HTableDescriptor hd = new HTableDescriptor(tableName);
            for (String fa : families) {
                hd.addFamily(new HColumnDescriptor(fa));
            }
            ha.createTable(hd);
        } catch (MasterNotRunningException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (ZooKeeperConnectionException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return true;
    }
    
    public String scanFamily(String tableName, byte[] familyName) {
        HTable ht;
        ResultScanner rs = null;
        StringBuilder sb = new StringBuilder();
        Configuration conf = HBaseConfiguration.create(new Configuration());
        try {
            ht = new HTable(conf, tableName);
            Scan s = new Scan();
            s.addFamily(familyName);
            rs = ht.getScanner(s);
            for (Result rr : rs) {
                sb.append(rr + "\n");
            }
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } finally {
            rs.close();
        }
        return sb.toString();       
    }
    
    public String scanQualifier(String tableName, byte[] familyName,
            byte[] qualifierName) {
        HTable ht;
        ResultScanner rs = null;
        StringBuilder sb = new StringBuilder();
        try {
            ht = new HTable(new Configuration(), tableName);
            Scan s = new Scan();
            s.addColumn(familyName,  qualifierName);
            rs = ht.getScanner(s);
            for (Result rr : rs) {
                sb.append(rr + "\n");
            }
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } finally {
            rs.close();
        }
        return sb.toString();  
    }
    
    public boolean writeTable(String tableName, String content,
            String rowName, String familyName, String columnName,
            String value) {
        Configuration conf = HBaseConfiguration.create(new Configuration());
        try {
            HTable ht = new HTable(conf, tableName);
            Put put = new Put(Bytes.toBytes(rowName));
            put.add(Bytes.toBytes(familyName), Bytes.toBytes(columnName),
                    Bytes.toBytes(value));
            ht.put(put);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        
        return true;
    }
    
    public boolean deleteTable(String tableName) {
        Configuration conf = HBaseConfiguration.create(new Configuration());
        try {
            HBaseAdmin ha = new HBaseAdmin(conf);
            ha.deleteTable(tableName);
        } catch (MasterNotRunningException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (ZooKeeperConnectionException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return true;
    }
    
}
