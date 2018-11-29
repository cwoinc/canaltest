package com.wantdo.demo.util;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.google.common.collect.Maps;
import com.wantdo.demo.HBaseEntity;
import org.apache.commons.lang.StringUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * @author king
 * @version 2018-11-22 9:46 AM
 */
public class ClientUtils {
    
    public static final String KAFKA_TOPIC, HBASE_NAMESPACE, INSTANCE_NAME = KAFKA_TOPIC = HBASE_NAMESPACE = "king";
    
    private static volatile Producer<String, String> producer = null;
    
    private static final String[] MYSQL_TEXT_TYPES = {"varchar", "longtext", "char", "datetime", "timestamp", "varbinary", "bit", "mediumtext", "set", "longblob", "text", "blob", "time", "date"};
    
    public static void printChangeData(CanalConnector connector, String kafkaTopic) {
        connector.connect();
        connector.subscribe(".*\\..*");
        connector.rollback();
        
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            connector.disconnect();
            if (null != producer) {
                producer.close();
            }
        }));
        
        while (true) {
            Message message = connector.getWithoutAck(10);
            long batchId = message.getId();
            if (batchId == -1 || message.getEntries().isEmpty()) {
                try {
                    Thread.sleep(3000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    Thread.currentThread().interrupt();
                }
                continue;
            }
            printEntries(message.getEntries(), kafkaTopic);
            connector.ack(batchId);
        }
    }
    
    private static void printEntries(List<CanalEntry.Entry> entries, String kafkaTopic) {
        entries.forEach(entry -> {
            if (!CanalEntry.EntryType.ROWDATA.equals(entry.getEntryType())) {
                return;
            }
            
            CanalEntry.RowChange rowChange;
            try {
                rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            
            CanalEntry.Header header = entry.getHeader();
            CanalEntry.EventType operateType = rowChange.getEventType();
            switch (operateType) {
                case INSERT:
                case UPDATE:
                    rowChange.getRowDatasList().forEach(rowData -> sendToKafka(kafkaTopic, header, operateType, rowData.getAfterColumnsList()));
                    break;
                case DELETE:
                    rowChange.getRowDatasList().forEach(rowData -> sendToKafka(kafkaTopic, header, operateType, rowData.getBeforeColumnsList()));
                    break;
                case TRUNCATE:
                    sendToKafka(kafkaTopic, header, operateType, null);
                    break;
                default:
                    throw new RuntimeException("暂不支持" + operateType + "该操作类型");
            }
        });
    }
    
    /**
     * 发送数据到kafka
     */
    private static void sendToKafka(String kafkaTopic, CanalEntry.Header header, CanalEntry.EventType operateType, List<CanalEntry.Column> columns) {
        Map<String, String> columnValueMap = Maps.newHashMap();
        if (null != columns) {
            columns.forEach(column -> columnValueMap.put(column.getName(), column.getValue()));
        }
        HBaseEntity entity = new HBaseEntity();
        entity.setOperateType(operateType);
        entity.setTableName(header.getTableName());
        entity.setDatabaseName(header.getSchemaName());
        entity.setColumnValueMap(columnValueMap);
        entity.setNameSpace(HBASE_NAMESPACE);
        
        initKafkaProducer();
        producer.send(new ProducerRecord<>(KAFKA_TOPIC, JsonMapper.toJsonString(entity)));
    }
    
    /**
     * 获取更新后的数据
     *
     * @param columns 列数据
     * @return 更新后的数据
     */
    private static Map<String, Object> getUpdateAfterData(List<CanalEntry.Column> columns) {
        Map<String, Object> columnValueMap = Maps.newHashMap();
        if (null != columns) {
            String intStr = "int";
            String decimalStr = "decimal";
            String floatStr = "float";
            String doubleStr = "double";
            String longStr = "long";
            String tinyintStr = "tinyint";
            columns.forEach(column -> {
                String mysqlType = column.getMysqlType();
                String value = column.getValue();
                Object obj = value;
                if (StringUtils.isNotBlank(value)) {
                    if (mysqlType.startsWith(intStr)) {
                        obj = Integer.valueOf(value);
                    } else if (mysqlType.startsWith(decimalStr)) {
                        obj = new BigDecimal(value);
                    } else if (mysqlType.startsWith(tinyintStr)) {
                        String falseStr = "0";
                        String trueStr = "1";
                        if (falseStr.equals(value)) {
                            obj = false;
                        } else if (trueStr.equals(value)) {
                            obj = true;
                        } else {
                            throw new RuntimeException(value + "该值在" + mysqlType + "该类型中转换失败!");
                        }
                    } else if (mysqlType.startsWith(longStr)) {
                        obj = Long.valueOf(value);
                    } else if (mysqlType.startsWith(floatStr) || mysqlType.startsWith(doubleStr)) {
                        obj = Double.valueOf(value);
                    } else {
                        boolean flag = true;
                        for (String mysqlTextType : MYSQL_TEXT_TYPES) {
                            if (mysqlType.startsWith(mysqlTextType)) {
                                flag = false;
                                break;
                            }
                        }
                        if (flag) {
                            throw new RuntimeException("暂不支持" + mysqlType + "该类型");
                        }
                    }
                }
                columnValueMap.put(column.getName(), obj);
            });
        }
        return columnValueMap;
    }
    
    /**
     * 初始化卡夫卡
     */
    private static synchronized void initKafkaProducer() {
        if (null != producer) {
            return;
        }
        
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "192.168.125.222:9092");
        properties.put("acks", "all");
        properties.put("retries", 0);
        properties.put("batch.size", 16384);
        properties.put("linger.ms", 1);
        properties.put("buffer.memory", 33554432);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        
        producer = new KafkaProducer<>(properties);
    }
    
}
