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

        String tablename = "iot_event_data";
        String[] familys = { "event", "terminal" };

        System.out.println("===========create table========");
        hBaseTbl.createHbaseTable(tablename,familys );

        System.out.println("===========insert records========");
        hBaseTbl.addHbaseRecord(tablename, "201512120000KKUBE1","event","startTime","0000");
        hBaseTbl.addHbaseRecord(tablename, "201512120000KKUBE2","event","startTime","0000");
        hBaseTbl.addHbaseRecord(tablename, "201512120000KKUBE3","event","startTime","0000");

        hBaseTbl.addHbaseRecord(tablename, "201512120010KKUBE1","event","startTime","0010");
        hBaseTbl.addHbaseRecord(tablename, "201512120020KKUBE1","event","startTime","0020");
        hBaseTbl.addHbaseRecord(tablename, "201512120030KKUBE1","event","startTime","0030");

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
        Result getresult = hBaseTbl.getHbaseRecord(tablename, "201512120000KKUBE1");

        for(KeyValue kv : getresult.raw()){
            System.out.print(new String(kv.getRow()) + " " );
            System.out.print(new String(kv.getFamily()) + ":" );
            System.out.print(new String(kv.getQualifier()) + " " );
            System.out.print(kv.getTimestamp() + " " );
            System.out.println(new String(kv.getValue()));
        }

        System.out.println("===========delete one record========");
        hBaseTbl.deleteHbaseRecord(tablename, "201512120000KKUBE2");

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

        System.out.println("===========delete table========");
        hBaseTbl.deleteHbaseTable("test_tbl");
    }
}
