package com.hadoop.hbase.interfaces;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;

import java.io.IOException;

/**
 * Created by TiroshanW on 12/14/2015.
 */
public interface ICreateHbaseTable {
    public HTableInterface createHbaseTable() throws IOException;
}
