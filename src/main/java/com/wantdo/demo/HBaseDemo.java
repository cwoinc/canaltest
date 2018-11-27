package com.wantdo.demo;

import com.wantdo.demo.util.ClientUtils;
import com.wantdo.demo.util.JsonMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;

import static com.wantdo.demo.util.HBaseUtil.*;

/**
 * @author king
 * @version 2018-11-24 2:53 PM
 */
public class HBaseDemo {
    
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
        kafkaConsumer.subscribe(Collections.singletonList(ClientUtils.KAFKA_TOPIC));
        while (true) {
            ConsumerRecords<String, String> records = kafkaConsumer.poll(3);
            for (ConsumerRecord<String, String> record : records) {
                HBaseEntity entity = (HBaseEntity) JsonMapper.fromJsonString(record.value(), HBaseEntity.class);
                exeHBaseOperate(entity);
            }
        }
        
    }
    
    private static void exeHBaseOperate(HBaseEntity entity) {
        initHBaseConnection();
        String tableName = entity.getHbaseTableName();
        createNamespace(entity.getNameSpace());
        createTable(tableName, entity.getFamily());
        switch (entity.getOperateType()) {
            case INSERT:
            case UPDATE:
                addData(tableName, entity.getFamily(), entity.getRowKey(), entity.getColumnValueMap());
                break;
            case DELETE:
                Map<String, String> result = getValue(tableName, entity.getFamily(), entity.getRowKey());
                if (null == result || 0 >= result.size()) {
                    throw new RuntimeException(tableName + " " + entity.getFamily() + " " + entity.getRowKey() + " 该数据不存在，无法删除！");
                }
                deleteRow(tableName, entity.getRowKey());
                break;
            case TRUNCATE:
                truncateTable(tableName);
                break;
            default:
                throw new RuntimeException(entity.getOperateType() + "该操作类型不存在");
        }
        
    }
    
}
