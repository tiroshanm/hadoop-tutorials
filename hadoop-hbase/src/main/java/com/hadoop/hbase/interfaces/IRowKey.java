package com.hadoop.hbase.interfaces;

import java.io.IOException;
import java.util.List;

/**
 * Created by TiroshanW on 12/14/2015.
 */
public interface IRowKey {

    public byte[] getRowKey() throws IOException;
    public void setList(List list);
}
