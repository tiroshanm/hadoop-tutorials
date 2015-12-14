package com.hadoop.hbase.core;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;

import java.io.IOException;


/**
 * Created by TiroshanW on 12/14/2015.
 */
public class Admin {

    private Configuration conf;

    public HBaseAdmin getAdmin() throws MasterNotRunningException, ZooKeeperConnectionException, IOException {
        // Instantiating configuration class
        System.out.println("Trying to connect...");
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "sandbox.hortonworks.com");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        conf.set("zookeeper.znode.parent", "/hbase-unsecure");


        // Instantiating HbaseAdmin class
        try{
            HBaseAdmin admin = new HBaseAdmin(conf);
            System.out.println("HBase is running!");
            return admin;
        }
        catch(MasterNotRunningException nre){
            nre.printStackTrace();
            throw nre;
        }
        catch(ZooKeeperConnectionException zre){
            zre.printStackTrace();
            throw zre;
        }
        catch(IOException ioe){
            ioe.printStackTrace();
            throw ioe;
        }

    }

    public Configuration getConfiguration (){
        return conf;
    }
}
