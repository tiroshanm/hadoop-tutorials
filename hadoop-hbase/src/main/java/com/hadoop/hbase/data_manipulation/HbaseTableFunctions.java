package com.hadoop.hbase.data_manipulation;

import com.hadoop.hbase.interfaces.IHbaseTableFunctions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by TiroshanW on 12/14/2015.
 */
public class HbaseTableFunctions implements IHbaseTableFunctions {
    private HBaseAdmin hBaseAdmin;
    private Configuration conf;

    public HbaseTableFunctions(HBaseAdmin admin){
        hBaseAdmin = admin;
        conf = hBaseAdmin.getConfiguration();
    }

    @Override
    public boolean createHbaseTable(String tableName, String[] familys) throws IOException {
        try{
            // Verifying the existance of the table
            if(this.existHbaseTable(tableName)){
                System.out.println("table "+tableName+" already exists!");
                return true;
            }
            else{
                // Instantiating table descriptor class
                HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));

                // Adding column families to table descriptor
                for (int i = 0; i < familys.length; i++) {
                    tableDescriptor.addFamily(new HColumnDescriptor(familys[i]));
                }

                // Execute the table through admin
                try{
                    hBaseAdmin.createTable(tableDescriptor);
                    System.out.println("create table " + tableName + " ok.");
                    return true;
                }
                catch (IOException ioe){
                    ioe.printStackTrace();
                    throw ioe;
                }
            }
        }
        catch (IOException ioeCeck){
            ioeCeck.printStackTrace();
            throw ioeCeck;
        }
    }

    @Override
    public boolean existHbaseTable(String tableName) throws IOException {
        try{
            // Verifying the existance of the table
            boolean tableExists = hBaseAdmin.tableExists(tableName);
            if(tableExists){
                return true;
            }
            else{
                return false;
            }
        }
        catch (IOException ioeCeck){
            ioeCeck.printStackTrace();
            throw ioeCeck;
        }
    }

    @Override
    public boolean deleteHbaseTable(String tableName) throws IOException {
        try {
            hBaseAdmin.disableTable(tableName);
            hBaseAdmin.deleteTable(tableName);
            System.out.println("delete table " + tableName + " ok.");
            return true;
        } catch (MasterNotRunningException mnre) {
            mnre.printStackTrace();
            throw mnre;
        } catch (ZooKeeperConnectionException zkce) {
            zkce.printStackTrace();
            throw zkce;
        }
    }

    @Override
    public boolean addHbaseRecord(String tableName, String rowKey, String family, String qualifier, String value) throws IOException {
        try {
            HTable table = new HTable(conf, tableName);
            Put put = new Put(Bytes.toBytes(rowKey));
            put.add(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes
                    .toBytes(value));
            table.put(put);
            System.out.println("insert recored " + rowKey + " to table "
                    + tableName + " ok.");
            return true;
        } catch (IOException e) {
            e.printStackTrace();
            throw e;
        }
    }

    @Override
    public boolean deleteHbaseRecord(String tableName, String rowKey) throws IOException {
        try{
            HTable table = new HTable(conf, tableName);
            List<Delete> list = new ArrayList<Delete>();
            Delete del = new Delete(rowKey.getBytes());
            list.add(del);
            table.delete(list);
            System.out.println("del recored " + rowKey + " ok.");
            return true;
        }catch (IOException e) {
            e.printStackTrace();
            throw e;
        }
    }

    @Override
    public Result getHbaseRecord(String tableName, String rowKey) throws IOException {
        try{
            HTable table = new HTable(conf, tableName);
            Get get = new Get(rowKey.getBytes());
            Result rs = table.get(get);
            return rs;
        }catch (IOException e) {
            e.printStackTrace();
            throw e;
        }

    }

    @Override
    public List getAllHbaseRecord(String tableName) throws IOException {
        try{
            HTable table = new HTable(conf, tableName);
            Scan s = new Scan();
            ResultScanner ss = table.getScanner(s);
            List<Result> rs = new ArrayList<Result>();

            for(Result r:ss){
                rs.add(r);
            }
            return rs;
        } catch (IOException e){
            e.printStackTrace();
            throw e;
        }
    }
}
