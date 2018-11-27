package com.wantdo.demo;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.wantdo.demo.util.ClientUtils;

import java.net.InetSocketAddress;

/**
 * @author king
 * @version 2018-11-24 9:13 AM
 */
public class CanalDemo {
    
    public static void main(String[] args) {
        
        CanalConnector connector = CanalConnectors.newSingleConnector(new InetSocketAddress("127.0.0.1", 11111), ClientUtils.INSTANCE_NAME, "", "");
        ClientUtils.printChangeData(connector, ClientUtils.KAFKA_TOPIC);
    }
    
}
