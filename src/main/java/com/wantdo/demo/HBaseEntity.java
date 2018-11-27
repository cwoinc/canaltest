package com.wantdo.demo;

import com.alibaba.otter.canal.protocol.CanalEntry;
import lombok.Data;
import org.apache.commons.lang.StringUtils;

import java.util.Map;

/**
 * @author king
 * @version 2018-11-24 11:38 AM
 */
@Data
public class HBaseEntity {
    
    private static final String DEFAULT_FAMILY = "i";
    
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
     * hbase表的行标识
     */
    private String rowKey;
    
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
}
