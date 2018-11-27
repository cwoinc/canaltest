package com.wantdo;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import org.apache.commons.lang.SystemUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.CollectionUtils;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

/**
 * Description
 * <p>
 * </p>
 * DATE 17/10/19.
 *
 * @author liuguanqing.
 */
public abstract class AbstractCanalConsumer implements InitializingBean {
    
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractCanalConsumer.class);
    protected Thread.UncaughtExceptionHandler handler = (t, e) -> LOGGER.error("parse events has an error", e);
    protected static final String SEP = SystemUtils.LINE_SEPARATOR;
    protected static String contextFormat = null;
    protected static String rowFormat = null;
    protected static String transactionFormat = null;
    protected static final String DATE_FORMAT = "yyyy-MM-dd HH:mm:ss";
    
    static {
        StringBuilder sb = new StringBuilder();
        sb.append(SEP)
                .append("-------------Batch-------------")
                .append(SEP)
                .append("* Batch Id: [{}] ,count : [{}] , Mem size : [{}] , Time : {}")
                .append(SEP)
                .append("* Start : [{}] ")
                .append(SEP)
                .append("* End : [{}] ")
                .append(SEP)
                .append("-------------------------------")
                .append(SEP);
        contextFormat = sb.toString();
        
        sb = new StringBuilder();
        sb.append(SEP)
                .append("+++++++++++++Row+++++++++++++>>>")
                .append("binlog[{}:{}] , name[{},{}] , eventType : {} , executeTime : {} , delay : {}ms")
                .append(SEP);
        rowFormat = sb.toString();
        
        sb = new StringBuilder();
        sb.append(SEP)
                .append("===========Transaction {} : {}=======>>>")
                .append("binlog[{}:{}] , executeTime : {} , delay : {}ms")
                .append(SEP);
        transactionFormat = sb.toString();
    }
    
    private volatile boolean running = false;
    protected Thread thread;
    
    private String zkServers;//cluster
    private String address;//single，ip:port
    private String destination;
    private String username;
    private String password;
    private int batchSize = 1024;//
    
    private String filter = "";//同canal filter，用于过滤database或者table的相关数据。
    
    private boolean debug = false;//开启debug，会把每条消息的详情打印
    
    /**
     * 1:retry，重试，重试默认为3次，由retryTimes参数决定，如果重试次数达到阈值，则跳过，并且记录日志。
     * 2:ignore,直接忽略，不重试，记录日志。
     */
    private int exceptionStrategy = 1;
    private int retryTimes = 3;
    
    private int waitingTime = 1000;//当binlog没有数据时，主线程等待的时间，单位ms,大于0
    
    private CanalConnector connector;
    
    public String getZkServers() {
        return zkServers;
    }
    
    public void setZkServers(String zkServers) {
        this.zkServers = zkServers;
    }
    
    public String getAddress() {
        return address;
    }
    
    public void setAddress(String address) {
        this.address = address;
    }
    
    public String getDestination() {
        return destination;
    }
    
    public void setDestination(String destination) {
        this.destination = destination;
    }
    
    public String getUsername() {
        return username;
    }
    
    public void setUsername(String username) {
        this.username = username;
    }
    
    public String getPassword() {
        return password;
    }
    
    public void setPassword(String password) {
        this.password = password;
    }
    
    public int getBatchSize() {
        return batchSize;
    }
    
    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }
    
    public boolean isDebug() {
        return debug;
    }
    
    public void setDebug(boolean debug) {
        this.debug = debug;
    }
    
    public int getRetryTimes() {
        return retryTimes;
    }
    
    public void setRetryTimes(int retryTimes) {
        this.retryTimes = retryTimes;
    }
    
    public int getExceptionStrategy() {
        return exceptionStrategy;
    }
    
    public void setExceptionStrategy(int exceptionStrategy) {
        this.exceptionStrategy = exceptionStrategy;
    }
    
    public String getFilter() {
        return filter;
    }
    
    public void setFilter(String filter) {
        this.filter = filter;
    }
    
    public int getWaitingTime() {
        return waitingTime;
    }
    
    public void setWaitingTime(int waitingTime) {
        this.waitingTime = waitingTime;
    }
    
    /**
     * 强烈建议捕获异常
     *
     * @param header
     * @param afterColumns
     */
    public abstract void insert(CanalEntry.Header header, List<CanalEntry.Column> afterColumns);
    
    /**
     * 强烈建议捕获异常
     *
     * @param header
     * @param beforeColumns 变化之前的列数据
     * @param afterColumns  变化之后的列数据
     */
    public abstract void update(CanalEntry.Header header, List<CanalEntry.Column> beforeColumns, List<CanalEntry.Column> afterColumns);
    
    /**
     * 强烈建议捕获异常
     *
     * @param header
     * @param beforeColumns 删除之前的列数据
     */
    public abstract void delete(CanalEntry.Header header, List<CanalEntry.Column> beforeColumns);
    
    /**
     * 创建表
     *
     * @param header 可以从header中获得schema、table的名称
     * @param sql
     */
    public void createTable(CanalEntry.Header header, String sql) {
        String schema = header.getSchemaName();
        String table = header.getTableName();
        LOGGER.info("parse event,create table,schema: {},table: {},SQL: {}", (Object) new String[]{schema, table, sql});
    }
    
    /**
     * 修改表结构,即alter指令，需要声明：通过alter增加索引、删除索引，也是此操作。
     *
     * @param header 可以从header中获得schema、table的名称
     * @param sql
     */
    public void alterTable(CanalEntry.Header header, String sql) {
        String schema = header.getSchemaName();
        String table = header.getTableName();
        LOGGER.info("parse event,alter table,schema: {},table: {},SQL: {}", (Object) new String[]{schema, table, sql});
    }
    
    /**
     * 清空、重建表
     *
     * @param header 可以从header中获得schema、table的名称
     * @param sql
     */
    public void truncateTable(CanalEntry.Header header, String sql) {
        String schema = header.getSchemaName();
        String table = header.getTableName();
        LOGGER.info("parse event,truncate table,schema: {},table: {},SQL: {}", (Object) new String[]{schema, table, sql});
    }
    
    /**
     * 重命名schema或者table，注意
     *
     * @param header 可以从header中获得schema、table的名称
     * @param sql
     */
    public void rename(CanalEntry.Header header, String sql) {
        String schema = header.getSchemaName();
        String table = header.getTableName();
        LOGGER.info("parse event,rename table,schema: {},table: {},SQL: {}", (Object) new String[]{schema, table, sql});
    }
    
    /**
     * 创建索引,通过“create index on table”指令
     *
     * @param header 可以从header中获得schema、table的名称
     * @param sql
     */
    public void createIndex(CanalEntry.Header header, String sql) {
        String schema = header.getSchemaName();
        String table = header.getTableName();
        LOGGER.info("parse event,create index,schema: {},table: {},SQL: {}", (Object) new String[]{schema, table, sql});
    }
    
    /**
     * 删除索引，通过“delete index on table”指令
     *
     * @param header * 可以从header中获得schema、table的名称
     * @param sql
     */
    public void deleteIndex(CanalEntry.Header header, String sql) {
        String schema = header.getSchemaName();
        String table = header.getTableName();
        LOGGER.info("parse event,delete table,schema: {},table: {},SQL: {}", (Object) new String[]{schema, table, sql});
    }
    
    /**
     * 强烈建议捕获异常，非上述已列出的其他操作，非核心
     * 除了“insert”、“update”、“delete”操作之外的，其他类型的操作.
     * 默认实现为“无操作”
     *
     * @param entry
     */
    public void whenOthers(CanalEntry.Entry entry) {
    }
    
    @Override
    public void afterPropertiesSet() throws Exception {
        if (waitingTime <= 0) {
            throw new IllegalArgumentException("waitingTime must be greater than 0");
        }
        if (ExceptionStrategy.codeOf(exceptionStrategy) == null) {
            throw new IllegalArgumentException("exceptionStrategy is not valid,1 or 2");
        }
        start();
    }
    
    public synchronized void start() {
        if (running) {
            return;
        }
        
        if (zkServers != null && zkServers.length() > 0) {
            connector = CanalConnectors.newClusterConnector(zkServers, destination, username, password);
        } else if (address != null) {
            String[] segments = address.split(":");
            SocketAddress socketAddress = new InetSocketAddress(segments[0], Integer.valueOf(segments[1]));
            connector = CanalConnectors.newSingleConnector(socketAddress, destination, username, password);
        } else {
            throw new IllegalArgumentException("zkServers or address cant be null at same time,you should specify one of them!");
        }
        
        thread = new Thread(this::process);
        
        thread.setUncaughtExceptionHandler(handler);
        thread.start();
        running = true;
    }
    
    protected synchronized void stop() {
        if (!running) {
            return;
        }
        running = false;//process()将会在下一次loop时退出
        if (thread != null) {
            thread.interrupt();
            try {
                thread.join();
            } catch (InterruptedException e) {
                // ignore
            }
        }
        MDC.remove("destination");
    }
    
    /**
     * 用于控制当连接异常时，重试的策略，我们不应该每次都是立即重试，否则将可能导致大量的错误，在空转时导致CPU过高的问题
     * sleep策略基于简单的累加，最长不超过3S
     */
    private void sleepWhenFailed(int times) {
        if (times <= 0) {
            return;
        }
        try {
            int sleepTime = 1000 + times * 100;//最大sleep 3s。
            Thread.sleep(sleepTime);
        } catch (Exception ex) {
            //
        }
    }
    
    protected void process() {
        int times = 0;
        while (running) {
            try {
                sleepWhenFailed(times);
                //after block,should check the status of thread.
                if (!running) {
                    break;
                }
                MDC.put("destination", destination);
                connector.connect();
                connector.subscribe(filter);
                times = 0;//reset;
                while (running) {
                    Message message = connector.getWithoutAck(batchSize); // 获取指定数量的数据，不确认
                    long batchId = message.getId();
                    int size = message.getEntries().size();
                    if (batchId == -1 || size == 0) {
                        try {
                            Thread.sleep(waitingTime);
                        } catch (InterruptedException e) {
                            //
                        }
                        continue;
                    }
                    //logger
                    printBatch(message, batchId);
                    
                    //遍历每条消息
                    for (CanalEntry.Entry entry : message.getEntries()) {
                        session(entry);//no exception
                    }
                    //ack all the time。
                    connector.ack(batchId);
                }
            } catch (Exception e) {
                LOGGER.error("process error!", e);
                if (times > 20) {
                    times = 0;
                }
                times++;
            } finally {
                connector.disconnect();
                MDC.remove("destination");
            }
        }
    }
    
    protected void session(CanalEntry.Entry entry) {
        CanalEntry.EntryType entryType = entry.getEntryType();
        int times = 0;
        boolean success = false;
        while (!success) {
            if (times > 0) {
                /**
                 * 1:retry，重试，重试默认为3次，由retryTimes参数决定，如果重试次数达到阈值，则跳过，并且记录日志。
                 * 2:ignore,直接忽略，不重试，记录日志。
                 */
                if (exceptionStrategy == ExceptionStrategy.RETRY.code) {
                    if (times >= retryTimes) {
                        break;
                    }
                } else {
                    break;
                }
            }
            try {
                switch (entryType) {
                    case TRANSACTIONBEGIN:
                        transactionBegin(entry);
                        break;
                    case TRANSACTIONEND:
                        transactionEnd(entry);
                        break;
                    case ROWDATA:
                        rowData(entry);
                        break;
                    default:
                        break;
                }
                success = true;
            } catch (Exception e) {
                times++;
                LOGGER.error("parse event has an error ,times: + " + times + ", data:" + entry.toString(), e);
            }
            
        }
        
        if (debug && success) {
            LOGGER.info("parse event success,position:" + entry.getHeader().getLogfileOffset());
        }
    }
    
    private void rowData(CanalEntry.Entry entry) throws Exception {
        CanalEntry.RowChange rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
        CanalEntry.EventType eventType = rowChange.getEventType();
        CanalEntry.Header header = entry.getHeader();
        long executeTime = header.getExecuteTime();
        long delayTime = new Date().getTime() - executeTime;
        String sql = rowChange.getSql();
        if (debug) {
            if (eventType == CanalEntry.EventType.QUERY || rowChange.getIsDdl()) {
                LOGGER.info("------SQL----->>> type : {} , sql : {} ", new Object[]{eventType.getNumber(), sql});
            }
            LOGGER.info(rowFormat,
                    new Object[]{
                            header.getLogfileName(),
                            String.valueOf(header.getLogfileOffset()),
                            header.getSchemaName(),
                            header.getTableName(),
                            eventType,
                            String.valueOf(executeTime),
                            String.valueOf(delayTime)
                    });
        }
        
        try {
            //对于DDL，直接执行，因为没有行变更数据
            switch (eventType) {
                case CREATE:
                    createTable(header, sql);
                    return;
                case ALTER:
                    alterTable(header, sql);
                    return;
                case TRUNCATE:
                    truncateTable(header, sql);
                    return;
                case ERASE:
                    LOGGER.debug("parse event : erase,ignored!");
                    return;
                case QUERY:
                    LOGGER.debug("parse event : query,ignored!");
                    return;
                case RENAME:
                    rename(header, sql);
                    return;
                case CINDEX:
                    createIndex(header, sql);
                    return;
                case DINDEX:
                    deleteIndex(header, sql);
                    return;
                default:
                    break;
            }
            //对于有行变更操作的
            for (CanalEntry.RowData rowData : rowChange.getRowDatasList()) {
                switch (eventType) {
                    case DELETE:
                        delete(header, rowData.getBeforeColumnsList());
                        break;
                    case INSERT:
                        insert(header, rowData.getAfterColumnsList());
                        break;
                    case UPDATE:
                        update(header, rowData.getBeforeColumnsList(), rowData.getAfterColumnsList());
                        break;
                    default:
                        whenOthers(entry);
                }
            }
        } catch (Exception e) {
            LOGGER.error("process event error ,", e);
            LOGGER.error(rowFormat,
                    new Object[]{
                            header.getLogfileName(),
                            String.valueOf(header.getLogfileOffset()),
                            header.getSchemaName(),
                            header.getTableName(),
                            eventType,
                            String.valueOf(executeTime),
                            String.valueOf(delayTime)
                    });
            throw e;//重新抛出
        }
    }
    
    /**
     * default，only logging information
     *
     * @param entry
     */
    public void transactionBegin(CanalEntry.Entry entry) {
        if (!debug) {
            return;
        }
        try {
            CanalEntry.TransactionBegin begin = CanalEntry.TransactionBegin.parseFrom(entry.getStoreValue());
            // 打印事务头信息，执行的线程id，事务耗时
            CanalEntry.Header header = entry.getHeader();
            long executeTime = header.getExecuteTime();
            long delayTime = new Date().getTime() - executeTime;
            LOGGER.info(transactionFormat,
                    new Object[]{
                            "begin",
                            begin.getTransactionId(),
                            header.getLogfileName(),
                            String.valueOf(header.getLogfileOffset()),
                            String.valueOf(header.getExecuteTime()),
                            String.valueOf(delayTime)
                    });
        } catch (Exception e) {
            LOGGER.error("parse event has an error , data:" + entry.toString(), e);
        }
    }
    
    public void transactionEnd(CanalEntry.Entry entry) {
        if (!debug) {
            return;
        }
        try {
            CanalEntry.TransactionEnd end = CanalEntry.TransactionEnd.parseFrom(entry.getStoreValue());
            // 打印事务提交信息，事务id
            CanalEntry.Header header = entry.getHeader();
            long executeTime = header.getExecuteTime();
            long delayTime = new Date().getTime() - executeTime;
            LOGGER.info(transactionFormat,
                    new Object[]{
                            "end",
                            end.getTransactionId(),
                            header.getLogfileName(),
                            String.valueOf(header.getLogfileOffset()),
                            String.valueOf(header.getExecuteTime()),
                            String.valueOf(delayTime)
                    });
        } catch (Exception e) {
            LOGGER.error("parse event has an error , data:" + entry.toString(), e);
        }
    }
    
    /**
     * 打印当前batch的摘要信息
     *
     * @param message
     * @param batchId
     */
    protected void printBatch(Message message, long batchId) {
        List<CanalEntry.Entry> entries = message.getEntries();
        if (CollectionUtils.isEmpty(entries)) {
            return;
        }
        
        long memSize = 0;
        for (CanalEntry.Entry entry : entries) {
            memSize += entry.getHeader().getEventLength();
        }
        int size = entries.size();
        String startPosition = buildPosition(entries.get(0));
        String endPosition = buildPosition(message.getEntries().get(size - 1));
        
        SimpleDateFormat format = new SimpleDateFormat(DATE_FORMAT);
        LOGGER.info(contextFormat, new Object[]{
                batchId,
                size,
                memSize,
                format.format(new Date()),
                startPosition,
                endPosition}
        );
    }
    
    protected String buildPosition(CanalEntry.Entry entry) {
        CanalEntry.Header header = entry.getHeader();
        long time = header.getExecuteTime();
        Date date = new Date(time);
        SimpleDateFormat format = new SimpleDateFormat(DATE_FORMAT);
        StringBuilder sb = new StringBuilder();
        sb.append(header.getLogfileName())
                .append(":")
                .append(header.getLogfileOffset())
                .append(":")
                .append(header.getExecuteTime())
                .append("(")
                .append(format.format(date))
                .append(")");
        return sb.toString();
    }
    
    enum ExceptionStrategy {
        RETRY(1),
        IGNORE(2);
        int code;
        
        ExceptionStrategy(int code) {
            this.code = code;
        }
        
        public static ExceptionStrategy codeOf(Integer code) {
            if (code == null) {
                return null;
            }
            for (ExceptionStrategy e : values()) {
                if (e.code == code) {
                    return e;
                }
            }
            return null;
        }
    }
}