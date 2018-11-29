package com.wantdo.demo;

import com.alibaba.otter.canal.protocol.CanalEntry;
import lombok.Data;
import org.apache.commons.lang.StringUtils;

import java.util.Map;
import java.util.Optional;

/**
 * @author king
 * @version 2018-11-24 11:38 AM
 */
@Data
public class HBaseEntity {
    
    /**
     * 默认的列族
     */
    private static final String DEFAULT_FAMILY = "i";
    
    /**
     * 默认的数据库主键名称
     */
    private static final String DEFAULT_KEY_NAME = "id";
    
    /**
     * hbase表名
     */
    private String tableName;
    
    /**
     * hbase nameSpace
     */
    private String nameSpace;
    
    /**
     * mysql数据库名称
     */
    private String databaseName;
    
    /**
     * mysql表的主键名称
     */
    private String keyName = DEFAULT_KEY_NAME;
    
    /**
     * hbase表的列族
     */
    private String family = DEFAULT_FAMILY;
    
    /**
     * 操作类型，如插入、更新、删除
     */
    private CanalEntry.EventType operateType;
    
    /**
     * 列名及其数据
     */
    private Map<String, String> columnValueMap;
    
    /**
     * 获取hbase里的表名
     *
     * @return hbase表名
     */
    public String getHbaseTableName() {
        if (StringUtils.isBlank(nameSpace)) {
            throw new RuntimeException("kafkaTopic不能为空");
        }
        if (StringUtils.isBlank(databaseName)) {
            throw new RuntimeException("数据库名不能为空");
        }
        if (StringUtils.isBlank(tableName)) {
            throw new RuntimeException("表名不能为空");
        }
        return nameSpace + ":" + databaseName + "." + tableName;
    }
    
    /**
     * 获取HBase里的rowKey
     *
     * @return rowKey
     */
    public String getRowKey() {
        Optional.ofNullable(columnValueMap).orElseThrow(() -> new RuntimeException("列名及其数据不能为空"));
        String rowKey = columnValueMap.get(keyName);
        if (StringUtils.isBlank(rowKey)) {
            throw new RuntimeException("rowKey不能为空");
        }
        return rowKey;
    }
}
