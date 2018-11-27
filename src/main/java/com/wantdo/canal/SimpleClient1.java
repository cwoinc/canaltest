package com.wantdo.canal;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;

import java.net.InetSocketAddress;

/**
 * @author jinxl
 */
public class SimpleClient1 {
    
    public static void main(String[] args) {
        CanalConnector connector = CanalConnectors.newSingleConnector(new InetSocketAddress("127.0.0.1", 11111), "king", "", "");
        //ClientUtils.printChangeData(connector);
    }
    
}