package com.hadoop.hbase.data_manipulation;

import com.hadoop.hbase.interfaces.ICreateHbaseTable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;

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
    public HTable createHbaseTable() throws IOException {
        Configuration conf = hBaseAdmin.getConfiguration();

//        try{
//            HTableInterface event_data = new HTable( conf, "event_data");
//            return (HTable) event_data;
//        }
//        catch (IOException ioeCeck){
            // Instantiating table descriptor class
            HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf("event_data"));

            // Adding column families to table descriptor
            tableDescriptor.addFamily(new HColumnDescriptor("event"));
            tableDescriptor.addFamily(new HColumnDescriptor("terminal"));

            // Execute the table through admin
            try{
                hBaseAdmin.createTable(tableDescriptor);
                System.out.println(" Table created ");

                try{
                    HTableInterface event_data = new HTable( conf, "event_data");
                    return (HTable) event_data;
                }
                catch (IOException ioe){
                    ioe.printStackTrace();
                    throw ioe;
                }
            }
            catch (IOException ioe){
                ioe.printStackTrace();
                throw ioe;
            }
//        }
    }
}
