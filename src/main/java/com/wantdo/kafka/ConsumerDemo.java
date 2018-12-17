package com.wantdo.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

/**
 * @author jinxl
 */
public class ConsumerDemo {
    
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "192.168.125.222:9092");
        properties.put("group.id", "group-1");
        properties.put("enable.auto.commit", "true");
        properties.put("auto.commit.interval.ms", "1000");
        properties.put("auto.offset.reset", "earliest");
        properties.put("session.timeout.ms", "30000");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties);
        kafkaConsumer.subscribe(Arrays.asList("maxwell_test"));
        while (true) {
            ConsumerRecords<String, String> records = kafkaConsumer.poll(3);
            for (ConsumerRecord<String, String> record : records) {
                System.out.println(record.topic());
            }
        }
        
    }
}