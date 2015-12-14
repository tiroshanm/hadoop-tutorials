package com.hadoop.hbase.core;

import com.hadoop.hbase.data_manipulation.CreateHbaseTable;
import com.hadoop.hbase.interfaces.ICreateHbaseTable;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;

import java.io.IOException;

/**
 * Created by TiroshanW on 12/14/2015.
 */
public class HBaseTest {
    private static HBaseAdmin hBaseAdmin;


    public static void main(String[] args) throws IOException {
        hBaseAdmin = new Admin().getAdmin();
        ICreateHbaseTable createTbl = new CreateHbaseTable(hBaseAdmin);
        HTable event_data_tbl = (HTable) createTbl.createHbaseTable();
    }
}
