package com.hadoop.hbase.data_manipulation;

import com.hadoop.hbase.interfaces.ICreateHbaseTable;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;

import java.io.IOException;

/**
 * Created by TiroshanW on 12/14/2015.
 */
public class CreateHbaseTable implements ICreateHbaseTable{
    private HBaseAdmin hBaseAdmin;

    public CreateHbaseTable(HBaseAdmin admin){
        hBaseAdmin = admin;
    }

    @Override
    public boolean createHbaseTable() throws IOException {
        try{
            // Verifying the existance of the table
            boolean tableExists = hBaseAdmin.tableExists("iot_event_data");
            if(tableExists){
                return true;
            }
            else{
                // Instantiating table descriptor class
                HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf("iot_event_data"));

                // Adding column families to table descriptor
                tableDescriptor.addFamily(new HColumnDescriptor("event"));
                tableDescriptor.addFamily(new HColumnDescriptor("terminal"));

                // Execute the table through admin
                try{
                    hBaseAdmin.createTable(tableDescriptor);
                    System.out.println(" Table created ");
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
}
