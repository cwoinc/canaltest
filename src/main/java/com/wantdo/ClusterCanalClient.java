package com.wantdo;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.kafka.clients.producer.KafkaProducer;

import java.net.InetSocketAddress;
import java.util.Properties;

public class ClusterCanalClient extends AbstractCanalClient {
    
    public ClusterCanalClient(String destination) {
        super(destination);
    }
    
    public static void main(String args[]) {
        String destination = "example";//"example";
        String topic = "kafka_topic";
//        String canalhazk = null;
        String kafka = "localhost:9092";
        String hostname = "localhost";
        String table = "lightning_deals";

//        if (args.length != 5) {
//            logger.error("input param must : hostname destination topic kafka table" +
//                    "for example: localhost example topic 192.168.0.163:9092 tablname");
//            System.exit(1);
//        } else {
//            hostname = args[0];
//            destination = args[1];
//            topic = args[2];
////            canalhazk = args[2];
//            kafka = args[3];
//            table = args[4];
//        }
        
        // 基于zookeeper动态获取canal server的地址，建立链接，其中一台server发生crash，可以支持failover
        CanalConnector connector = CanalConnectors.newSingleConnector(new InetSocketAddress(hostname, 11111), destination, "", "");
//        CanalConnector connector = CanalConnectors.newClusterConnector(
//                canalhazk, destination, "userName", "passwd");
        Properties props = new Properties();
        
        props.put("bootstrap.servers", kafka);
        props.put("request.required.acks", 1);
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);//32m
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        
        final ClusterCanalClient clientTest = new ClusterCanalClient(destination);
        clientTest.setConnector(connector);
        clientTest.setKafkaProducer(producer);
        clientTest.setKafkaTopic(topic);
        clientTest.setFilterTable(table);
        clientTest.start();
        
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                logger.info("## stop the canal client");
                clientTest.stop();
            } catch (Throwable e) {
                logger.warn("##something goes wrong when stopping canal:\n{}", ExceptionUtils.getFullStackTrace(e));
            } finally {
                logger.info("## canal client is down.");
            }
        }));
    }
}