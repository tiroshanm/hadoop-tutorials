package com.hadoop.hbase.core;

import com.hadoop.hbase.data_manipulation.HbaseTableFunctions;
import com.hadoop.hbase.interfaces.IHbaseTableFunctions;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Result;

import java.io.IOException;
import java.util.List;

/**
 * Created by TiroshanW on 12/14/2015.
 */
public class HBaseTest {
    private static HBaseAdmin hBaseAdmin;


    public static void main(String[] args) throws IOException {
        hBaseAdmin = new Admin().getAdmin();
        IHbaseTableFunctions hBaseTbl = new HbaseTableFunctions(hBaseAdmin);

        String tablename = "testDataBase:testTable";
        String[] familys = { "data"};

        System.out.println("===========create table========");
        hBaseTbl.createHbaseTable(tablename,familys);

        System.out.println("===========insert records========");
        hBaseTbl.addHbaseRecord(tablename, "025".getBytes(),"data","code","0000");
        hBaseTbl.addHbaseRecord(tablename, "654".getBytes(),"data","code","0000");
        hBaseTbl.addHbaseRecord(tablename, "852".getBytes(),"data","code","0000");

        hBaseTbl.addHbaseRecord(tablename, "025".getBytes(),"data","name","ades");
        hBaseTbl.addHbaseRecord(tablename, "654".getBytes(),"data","name","fsfd");
        hBaseTbl.addHbaseRecord(tablename, "852".getBytes(),"data","name","ytrt");

        System.out.println("===========show all record========");
        List list = hBaseTbl.getAllHbaseRecord(tablename);

        for(Object result:list){
            Result r = (Result) result;
            for(KeyValue kv : r.raw()){
                System.out.print(new String(kv.getRow()) + " ");
                System.out.print(new String(kv.getFamily()) + ":");
                System.out.print(new String(kv.getQualifier()) + " ");
                System.out.print(kv.getTimestamp() + " ");
                System.out.println(new String(kv.getValue()));
            }
        }

        System.out.println("===========get one record========");
        Result getresult = hBaseTbl.getHbaseRecord(tablename, "100".getBytes());

        for(KeyValue kv : getresult.raw()){
            System.out.print(new String(kv.getRow()) + " " );
            System.out.print(new String(kv.getFamily()) + ":" );
            System.out.print(new String(kv.getQualifier()) + " " );
            System.out.print(kv.getTimestamp() + " " );
            System.out.println(new String(kv.getValue()));
        }

        System.out.println("===========delete one record========");
        hBaseTbl.deleteHbaseRecord(tablename, "100".getBytes());

        List Dellist = hBaseTbl.getAllHbaseRecord(tablename);

        for(Object result:Dellist){
            Result r = (Result) result;
            for(KeyValue kv : r.raw()){
                System.out.print(new String(kv.getRow()) + " ");
                System.out.print(new String(kv.getFamily()) + ":");
                System.out.print(new String(kv.getQualifier()) + " ");
                System.out.print(kv.getTimestamp() + " ");
                System.out.println(new String(kv.getValue()));
            }
        }

        System.out.println("===========show range scan records========");
        List rangelist = hBaseTbl.getRangeHbaseRecord(tablename, "010".getBytes(), "100".getBytes());

        for(Object result:rangelist){
            Result r = (Result) result;
            for(KeyValue kv : r.raw()){
                System.out.print(new String(kv.getRow()) + " ");
                System.out.print(new String(kv.getFamily()) + ":");
                System.out.print(new String(kv.getQualifier()) + " ");
                System.out.print(kv.getTimestamp() + " ");
                System.out.println(new String(kv.getValue()));
            }
        }

        System.out.println("===========delete table========");
        hBaseTbl.deleteHbaseTable(tablename);
    }
}
