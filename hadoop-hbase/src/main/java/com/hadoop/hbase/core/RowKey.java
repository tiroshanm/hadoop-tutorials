package com.hadoop.hbase.core;

import com.hadoop.hbase.interfaces.IRowKey;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;

/**
 * Created by TiroshanW on 12/14/2015.
 */
public class RowKey implements IRowKey{

    private List rowKeyDataList;

    @Override
    public byte[] getRowKey() throws IOException {
        ByteArrayOutputStream byteOutput = new ByteArrayOutputStream();
        DataOutputStream data = new DataOutputStream(byteOutput);

        Iterator rowKeyIterator = rowKeyDataList.iterator();
        while(rowKeyIterator.hasNext()){
            data.writeBytes(rowKeyIterator.next().toString());
        }
        return byteOutput.toByteArray();
    }

    @Override
    public void setList(List list) {
        rowKeyDataList = list;
    }
}
