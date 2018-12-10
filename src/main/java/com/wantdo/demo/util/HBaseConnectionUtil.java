package com.wantdo.demo.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

/**
 * @author king
 * @version 2018-12-08 7:06 PM
 */
public class HBaseConnectionUtil {
    
    public static final String TABLE_NAME = "wantdo:az_mws.fe_charge_component";
    
    public static final String BASE_PATH = "/home/king/diy/shared_folders/";
    
    public static final String INPUT_PATH = BASE_PATH + "fe_safe_reimbursement.tsv";
    
    public static final String OUTPUT_PATH = BASE_PATH + String.valueOf(System.currentTimeMillis());
    
    public static Connection getHBaseConnection() {
        Connection conn;
        try {
            conn = ConnectionFactory.createConnection(getConfiguration());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        
        return conn;
    }
    
    public static Configuration getConfiguration() {
        Configuration config = HBaseConfiguration.create();
        //zookeeper集群
        String zookeeper = "192.168.125.212,192.168.125.211,192.168.125.210";
        config.set("hbase.zookeeper.quorum", zookeeper);
        return config;
    }
    
}
