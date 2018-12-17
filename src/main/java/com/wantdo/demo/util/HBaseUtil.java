package com.wantdo.demo.util;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

/**
 * @author jinxl
 * @date
 */
public class HBaseUtil {
    
    private static volatile Connection conn = null;
    
    private static final Set<String> HBASE_NAMESPACES = Sets.newHashSet();
    
    public static void main(String[] args) {
        initHBaseConnection();
        
        try (Admin admin = conn.getAdmin()) {
            NamespaceDescriptor[] namespaceDescriptors = admin.listNamespaceDescriptors();
            for (NamespaceDescriptor namespaceDescriptor : namespaceDescriptors) {
                System.out.println(namespaceDescriptor);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        String tableName = "wantdo:az_mws.fe_charge_component";
        //createTable(tableName,"i");
        getValue(tableName);
    }
    
    public static synchronized void initHBaseConnection() {
        if (null != conn) {
            return;
        }
        Configuration config = HBaseConfiguration.create();
        //zookeeper集群
        String zookeeper = "192.168.125.212,192.168.125.211,192.168.125.210";
        config.set("hbase.zookeeper.quorum", zookeeper);
        try {
            conn = ConnectionFactory.createConnection(config);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        
        try (Admin admin = conn.getAdmin()) {
            NamespaceDescriptor[] namespaceDescriptors = admin.listNamespaceDescriptors();
            for (NamespaceDescriptor namespaceDescriptor : namespaceDescriptors) {
                HBASE_NAMESPACES.add(namespaceDescriptor.getName());
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            if (null != conn) {
                try {
                    conn.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }));
    }
    
    /**
     * <建表>
     */
    public static void createTable(String tableName, String... families) {
        checkValues(tableName);
        TableName table = TableName.valueOf(tableName);
        try (Admin admin = conn.getAdmin()) {
            if (admin.tableExists(table)) {
                return;
            }
            if (0 == families.length) {
                throw new RuntimeException("建表时，至少需要指定一个列族");
            }
            HTableDescriptor descriptor = new HTableDescriptor(table);
            for (String family : families) {
                descriptor.addFamily(new HColumnDescriptor(family.getBytes()));
            }
            admin.createTable(descriptor);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    
    /**
     * <添加列族>
     */
    public static void addFamily(String tableName, String family) {
        checkValues(tableName, family);
        TableName table = TableName.valueOf(tableName);
        try (Admin admin = conn.getAdmin()) {
            HColumnDescriptor columnDesc = new HColumnDescriptor(family);
            admin.addColumn(table, columnDesc);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    
    /**
     * <添加数据>
     */
    public static void addData(String tableName, String family, String rowKey, Map<String, String> columns) {
        checkValues(tableName, family, rowKey);
        if (null == columns || 0 >= columns.size()) {
            throw new RuntimeException("行数据不能为空");
        }
        try (Table table = conn.getTable(TableName.valueOf(tableName))) {
            Put put = new Put(Bytes.toBytes(rowKey));
            columns.forEach((col, val) -> put.addColumn(Bytes.toBytes(family), Bytes.toBytes(col), Bytes.toBytes(val)));
            table.put(put);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    
    /**
     * 获取值
     */
    public static Map<String, String> getValue(String tableName, String family, String rowKey) {
        checkValues(tableName, family, rowKey);
        Map<String, String> resultMap = Maps.newHashMap();
        Get get = new Get(Bytes.toBytes(rowKey));
        get.addFamily(family.getBytes());
        try (Table table = conn.getTable(TableName.valueOf(tableName))) {
            if (null == table || !table.exists(get)) {
                return resultMap;
            }
            Result res = table.get(get);
            Map<byte[], byte[]> result = res.getFamilyMap(family.getBytes());
            result.forEach((key, value) -> resultMap.put(Bytes.toString(key), Bytes.toString(value)));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return resultMap;
    }
    
    /**
     * <根据rowKey和column获取数据>
     */
    public static String getValue(String tableName, String family, String rowKey, String column) {
        checkValues(tableName, family, rowKey, column);
        try (Table table = conn.getTable(TableName.valueOf(tableName))) {
            Get get = new Get(Bytes.toBytes(rowKey));
            get.addColumn(Bytes.toBytes(family), Bytes.toBytes(column));
            Result res = table.get(get);
            byte[] result = res.getValue(Bytes.toBytes(family), Bytes.toBytes(column));
            return Bytes.toString(result);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        
    }
    
    /**
     * <查询所有数据>
     */
    public static void getValue(String tableName) {
        checkValues(tableName);
        try (Table table = conn.getTable(TableName.valueOf(tableName))) {
            ResultScanner rs = table.getScanner(new Scan());
            rs.forEach(result -> {
                System.out.println("获得到rowKey:" + Bytes.toString(result.getRow()));
                for (Cell cell : result.rawCells()) {
                    System.out.println("列：" + Bytes.toString(CellUtil.cloneFamily(cell)) + "====值:" + Bytes.toString(CellUtil.cloneValue(cell)));
                }
            });
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    
    /**
     * <删除指定名称的列簇>
     */
    public static void deleteFamily(String tableName, String family) {
        checkValues(tableName, family);
        TableName table = TableName.valueOf(tableName);
        try (Admin admin = conn.getAdmin()) {
            admin.deleteColumn(table, Bytes.toBytes(family));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    
    /**
     * <删除行>
     */
    public static void deleteRow(String tableName, String rowKey) {
        checkValues(tableName, rowKey);
        try (Table table = conn.getTable(TableName.valueOf(tableName))) {
            table.delete(new Delete(Bytes.toBytes(rowKey)));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    
    /**
     * <删除表>
     */
    public static void dropTable(String tableName) {
        checkValues(tableName);
        TableName table = TableName.valueOf(tableName);
        try (Admin admin = conn.getAdmin()) {
            if (!admin.tableExists(table)) {
                throw new RuntimeException(tableName + "该数据库表信息不存在");
            }
            admin.disableTable(table);
            admin.deleteTable(table);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    
    /**
     * 创建命名空间
     *
     * @param namespace 命名空间
     */
    public static void createNamespace(String namespace) {
        checkValues(namespace);
        try (Admin admin = conn.getAdmin()) {
            if (HBASE_NAMESPACES.contains(namespace)) {
                return;
            }
            admin.createNamespace(NamespaceDescriptor.create(namespace).build());
            HBASE_NAMESPACES.add(namespace);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    
    /**
     * 删除命名空间
     *
     * @param namespace 命名空间
     */
    public static void dropNamespace(String namespace) {
        checkValues(namespace);
        try (Admin admin = conn.getAdmin()) {
            if (!HBASE_NAMESPACES.contains(namespace)) {
                throw new RuntimeException(namespace + "该命名空间不存在");
            }
            TableName[] tableNames = admin.listTableNamesByNamespace(namespace);
            for (TableName tableName : tableNames) {
                admin.disableTable(tableName);
                admin.deleteTable(tableName);
            }
            admin.deleteNamespace(namespace);
            HBASE_NAMESPACES.remove(namespace);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    
    /**
     * 清空表，并且不保留分割
     *
     * @param tableName 表名
     */
    public static void truncateTable(String tableName) {
        checkValues(tableName);
        TableName table = TableName.valueOf(tableName);
        try (Admin admin = conn.getAdmin()) {
            if (!admin.tableExists(table)) {
                throw new RuntimeException(tableName + "该数据库表信息不存在");
            }
            admin.disableTable(table);
            admin.truncateTable(table, false);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    
    /**
     * 重命名表名
     *
     * @param tableName    原表名
     * @param newTableName 新表名
     */
    public static void reNameTable(String tableName, String newTableName) {
        checkValues(tableName, newTableName);
        TableName table = TableName.valueOf(tableName);
        try (Admin admin = conn.getAdmin()) {
            if (!admin.tableExists(table)) {
                throw new RuntimeException(tableName + "该数据库表信息不存在");
            }
            admin.disableTable(table);
            String snapshotName = String.valueOf(System.currentTimeMillis());
            admin.snapshot(snapshotName, table);
            admin.cloneSnapshot(snapshotName, TableName.valueOf(newTableName));
            admin.deleteSnapshot(snapshotName);
            admin.deleteTable(table);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    
    /**
     * 校验数据是否为空
     *
     * @param values 数据
     */
    private static void checkValues(String... values) {
        for (String value : values) {
            if (StringUtils.isBlank(value)) {
                throw new RuntimeException("表名、列族、行标识等参数不能为空!");
            }
        }
    }
    
}