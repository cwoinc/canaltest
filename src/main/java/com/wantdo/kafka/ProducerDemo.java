package com.wantdo.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * @author jinxl
 */
public class ProducerDemo {
    
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "192.168.125.210:9092,192.168.125.211:9092,192.168.125.212:9092");
        properties.put("acks", "all");
        properties.put("retries", 0);
        properties.put("batch.size", 16384);
        properties.put("linger.ms", 1);
        properties.put("buffer.memory", 33554432);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        try (Producer<String, String> producer = new KafkaProducer<>(properties)) {
            for (int i = 0; i < 100; i++) {
                String msg = "Message " + i;
                producer.send(new ProducerRecord<>("test1", msg));
                System.out.println("Sent:" + msg);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        
    }
}
