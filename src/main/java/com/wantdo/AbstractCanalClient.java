package com.wantdo;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.protocol.CanalEntry.*;
import com.alibaba.otter.canal.protocol.Message;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.commons.lang.SystemUtils;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class AbstractCanalClient {

    protected final static Logger logger = LoggerFactory.getLogger(AbstractCanalClient.class);
    protected static final String SEP = SystemUtils.LINE_SEPARATOR;
    protected static final String DATE_FORMAT = "yyyy-MM-dd HH:mm:ss";
    protected volatile boolean running = false;
    protected Thread.UncaughtExceptionHandler handler = (t, e) -> logger.error("parse events has an error", e);
    protected Thread thread = null;
    protected CanalConnector connector;
    protected static String context_format = null;
    protected static String row_format = null;
    protected static String transaction_format = null;
    protected String destination;
    protected Producer<String, String> kafkaProducer = null;
    protected String topic;
    protected String table;

    static {
        context_format = SEP
                + "****************************************************" + SEP;
        context_format += "* Batch Id: [{}] ,count : [{}] , memsize : [{}] , Time : {}"
                + SEP;
        context_format += "* Start : [{}] " + SEP;
        context_format += "* End : [{}] " + SEP;
        context_format += "****************************************************"
                + SEP;

        row_format = SEP
                + "----------------> binlog[{}:{}] , name[{},{}] , eventType : {} , executeTime : {} , delay : {}ms"
                + SEP;

        transaction_format = SEP
                + "================> binlog[{}:{}] , executeTime : {} , delay : {}ms"
                + SEP;

    }

    public AbstractCanalClient(String destination) {
        this(destination, null);
    }

    public AbstractCanalClient(String destination, CanalConnector connector) {
        this(destination, connector, null);
    }

    public AbstractCanalClient(String destination, CanalConnector connector,
                               Producer<String, String> kafkaProducer) {
        this.connector = connector;
        this.destination = destination;
        this.kafkaProducer = kafkaProducer;
    }

    protected void start() {
        Assert.notNull(connector, "connector is null");
        Assert.notNull(kafkaProducer, "Kafka producer configuration is null");
        Assert.notNull(topic, "kafaka topic is null");
        Assert.notNull(table, "table is null");
        thread = new Thread(this::process);

        thread.setUncaughtExceptionHandler(handler);
        thread.start();
        running = true;
    }

    protected void stop() {
        if (!running) {
            return;
        }
        running = false;
        if (thread != null) {
            try {
                thread.join();
            } catch (InterruptedException e) {
                // ignore
            }
        }

        kafkaProducer.close();
        MDC.remove("destination");
    }

    protected void process() {
        int batchSize = 1024;
        //while (running) {
        try {
            MDC.put("destination", destination);
            connector.connect();
            connector.subscribe(".*\\..*");
            while (running) {
                Message message = connector.getWithoutAck(batchSize); // 获取指定数量的数据
                long batchId = message.getId();
                try {
                    int size = message.getEntries().size();
                    if (batchId == -1 || size == 0) {
                        try {
                            Thread.sleep(100);
                        } catch (InterruptedException e) {
                        }
                    } else {

                        kafkaEntry(message.getEntries());

                    }

                    connector.ack(batchId); // 提交确认
                } catch (Exception e) {
                    connector.rollback(batchId); // 处理失败, 回滚数据
                }
            }
        } catch (Exception e) {
            logger.error("process error!", e);
        } finally {
            connector.disconnect();
            MDC.remove("destination");
        }
        //}
    }

    private void printSummary(Message message, long batchId, int size) {
        long memsize = 0;
        for (Entry entry : message.getEntries()) {
            memsize += entry.getHeader().getEventLength();
        }

        String startPosition = null;
        String endPosition = null;
        if (!CollectionUtils.isEmpty(message.getEntries())) {
            startPosition = buildPositionForDump(message.getEntries().get(0));
            endPosition = buildPositionForDump(message.getEntries().get(
                    message.getEntries().size() - 1));
        }

        SimpleDateFormat format = new SimpleDateFormat(DATE_FORMAT);
        logger.info(context_format, new Object[]{batchId, size, memsize,
                format.format(new Date()), startPosition, endPosition});
    }

    protected String buildPositionForDump(Entry entry) {
        long time = entry.getHeader().getExecuteTime();
        Date date = new Date(time);
        SimpleDateFormat format = new SimpleDateFormat(DATE_FORMAT);
        return entry.getHeader().getLogfileName() + ":"
                + entry.getHeader().getLogfileOffset() + ":"
                + entry.getHeader().getExecuteTime() + "("
                + format.format(date) + ")";
    }

    private void kafkaEntry(List<Entry> entrys) throws InterruptedException, ExecutionException {
        for (Entry entry : entrys) {
            if (entry.getEntryType() == EntryType.TRANSACTIONBEGIN
                    || entry.getEntryType() == EntryType.TRANSACTIONEND) {
                continue;
            }

            RowChange rowChage = null;
            try {
                rowChage = RowChange.parseFrom(entry.getStoreValue());
            } catch (Exception e) {
                throw new RuntimeException(
                        "ERROR ## parser of eromanga-event has an error , data:"
                                + entry.toString(), e);
            }

            String logfileName = entry.getHeader().getLogfileName();
            Long logfileOffset = entry.getHeader().getLogfileOffset();
            String dbName = entry.getHeader().getSchemaName();
            String tableName = entry.getHeader().getTableName();

            EventType eventType = rowChage.getEventType();
            if (eventType == EventType.DELETE || eventType == EventType.UPDATE
                    || eventType == EventType.INSERT) {
                for (RowData rowData : rowChage.getRowDatasList()) {
                    String tmpstr = "";
                    if (eventType == EventType.DELETE) {
                        tmpstr = getDeleteJson(rowData.getBeforeColumnsList());
                    } else if (eventType == EventType.INSERT) {
                        tmpstr = getInsertJson(rowData.getAfterColumnsList());
                    } else if (eventType == EventType.UPDATE) {
                        tmpstr = getUpdateJson(rowData.getBeforeColumnsList(),
                                rowData.getAfterColumnsList());
                    } else {
                        continue;
                    }
                    logger.info(this.topic + tmpstr);
                    kafkaProducer.send(
                            new ProducerRecord<String, String>(this.topic,
                                    tmpstr)).get();
                }
            }
        }
    }

    private JSONObject columnToJson(List<Column> columns) {
        JSONObject json = new JSONObject();
        for (Column column : columns) {
            json.put(column.getName(), column.getValue());
        }
        return json;
    }

    private String getInsertJson(List<Column> columns) {
        JSONObject json = new JSONObject();
        json.put("type", "insert");
        json.put("data", this.columnToJson(columns));
        return json.toJSONString();
    }

    private String getUpdateJson(List<Column> befcolumns, List<Column> columns) {
        JSONObject json = new JSONObject();
        json.put("type", "update");
        json.put("data", this.columnToJson(columns));
        return json.toJSONString();
    }

    private String getDeleteJson(List<Column> columns) {
        JSONObject json = new JSONObject();
        json.put("type", "delete");
        json.put("data", this.columnToJson(columns));
        return json.toJSONString();
    }

    protected void printEntry(List<Entry> entrys) {
        for (Entry entry : entrys) {
            long executeTime = entry.getHeader().getExecuteTime();
            long delayTime = new Date().getTime() - executeTime;

            if (entry.getEntryType() == EntryType.TRANSACTIONBEGIN
                    || entry.getEntryType() == EntryType.TRANSACTIONEND) {
                if (entry.getEntryType() == EntryType.TRANSACTIONBEGIN) {
                    TransactionBegin begin = null;
                    try {
                        begin = TransactionBegin.parseFrom(entry
                                .getStoreValue());
                    } catch (InvalidProtocolBufferException e) {
                        throw new RuntimeException(
                                "parse event has an error , data:"
                                        + entry.toString(), e);
                    }
                    // 打印事务头信息，执行的线程id，事务耗时
                    logger.info(
                            transaction_format,
                            new Object[]{
                                    entry.getHeader().getLogfileName(),
                                    String.valueOf(entry.getHeader()
                                            .getLogfileOffset()),
                                    String.valueOf(entry.getHeader()
                                            .getExecuteTime()),
                                    String.valueOf(delayTime)});
                    logger.info(" BEGIN ----> Thread id: {}",
                            begin.getThreadId());
                } else {
                    TransactionEnd end = null;
                    try {
                        end = TransactionEnd.parseFrom(entry.getStoreValue());
                    } catch (InvalidProtocolBufferException e) {
                        throw new RuntimeException(
                                "parse event has an error , data:"
                                        + entry.toString(), e);
                    }
                    // 打印事务提交信息，事务id
                    logger.info("----------------\n");
                    logger.info(" END ----> transaction id: {}",
                            end.getTransactionId());
                    logger.info(
                            transaction_format,
                            new Object[]{
                                    entry.getHeader().getLogfileName(),
                                    String.valueOf(entry.getHeader()
                                            .getLogfileOffset()),
                                    String.valueOf(entry.getHeader()
                                            .getExecuteTime()),
                                    String.valueOf(delayTime)});
                }

                continue;
            }

            if (entry.getEntryType() == EntryType.ROWDATA) {
                RowChange rowChage = null;
                try {
                    rowChage = RowChange.parseFrom(entry.getStoreValue());
                } catch (Exception e) {
                    throw new RuntimeException(
                            "parse event has an error , data:"
                                    + entry.toString(), e);
                }

                EventType eventType = rowChage.getEventType();

                logger.info(
                        row_format,
                        new Object[]{
                                entry.getHeader().getLogfileName(),
                                String.valueOf(entry.getHeader()
                                        .getLogfileOffset()),
                                entry.getHeader().getSchemaName(),
                                entry.getHeader().getTableName(),
                                eventType,
                                String.valueOf(entry.getHeader()
                                        .getExecuteTime()),
                                String.valueOf(delayTime)});

                if (eventType == EventType.QUERY || rowChage.getIsDdl()) {
                    logger.info(" sql ----> " + rowChage.getSql() + SEP);
                    continue;
                }

                for (RowData rowData : rowChage.getRowDatasList()) {
                    if (eventType == EventType.DELETE) {
                        printColumn(rowData.getBeforeColumnsList());
                    } else if (eventType == EventType.INSERT) {
                        printColumn(rowData.getAfterColumnsList());
                    } else {
                        printColumn(rowData.getAfterColumnsList());
                    }
                }
            }
        }
    }

    protected void printColumn(List<Column> columns) {
        for (Column column : columns) {
            StringBuilder builder = new StringBuilder();
            builder.append(column.getName() + " : " + column.getValue());
            builder.append("    type=" + column.getMysqlType());
            if (column.getUpdated()) {
                builder.append("    update=" + column.getUpdated());
            }
            builder.append(SEP);
            logger.info(builder.toString());
        }
    }

    public void setConnector(CanalConnector connector) {
        this.connector = connector;
    }

    public void setKafkaProducer(Producer<String, String> kafkaProducer) {
        this.kafkaProducer = kafkaProducer;
    }

    public void setKafkaTopic(String topic) {
        this.topic = topic;
    }

    public void setFilterTable(String table) {
        this.table = table;
    }
}