package com.hadoop.hbase.interfaces;

import org.apache.hadoop.hbase.client.Result;

import java.io.IOException;
import java.util.List;

/**
 * Created by TiroshanW on 12/14/2015.
 */
public interface IHbaseTableFunctions {

    public boolean createHbaseTable(String tableName, String[] familys) throws IOException;
    public boolean existHbaseTable(String tableName) throws IOException;
    public boolean deleteHbaseTable(String tableName) throws IOException;

    public boolean addHbaseRecord(String tableName, byte[] rowKey,String family, String qualifier, String value) throws IOException;
    public boolean deleteHbaseRecord(String tableName, byte[] rowKey) throws IOException;
    public Result getHbaseRecord(String tableName, byte[] rowKey) throws IOException;
    public List getAllHbaseRecord(String tableName) throws IOException;

    public List getRangeHbaseRecord(String tableName, byte[] startkey, byte[] endkey)throws IOException;
}
