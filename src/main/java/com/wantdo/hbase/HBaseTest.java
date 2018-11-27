package com.wantdo.hbase;

import java.util.HashMap;
import java.util.Map;

import static com.wantdo.demo.util.HBaseUtil.*;

/**
 * @author king
 * @version 2018-11-24 9:16 AM
 */
public class HBaseTest {

    public static void main(String[] args) {
        String family = "family1";
        String tableName = "demo_table";
        initHBaseConnection();
        // 删除表
        dropTable(tableName);
        // 创建表
        createTable(tableName, family, "abc");
        addFamily(tableName, "king");
        addFamily(tableName, "family2");
        // 添加数据
        String row1 = "数据";
        Map<String, String> columns = new HashMap<>();
        columns.put("test_data1", "test_data1_数据_1");
        columns.put("test_data11", "test_data1_数据_2");
        columns.put("test_data2", "test_data1_数据_3");
        addData(tableName, family, row1, columns);
        addData(tableName, "family2", row1, columns);

        // 添加数据
        String row2 = "ttt";
        Map<String, String> columns2 = new HashMap<>();
        columns2.put("test_data1", "test_data1_ttt_1");
        columns2.put("test_data2", "test_data1_ttt_2");
        addData(tableName, family, row2, columns2);

        columns2.clear();
        columns2.put("1", "2");
        columns2.put("12", "23");
        addData(tableName, family, row2, columns2);

        // 条件查询 family + row
        Map<String, String> rowMap1 = getValue(tableName, family, row1);
        System.out.println("查" + family + ":" + row1 + "=============================");
        for (Map.Entry<String, String> entry : rowMap1.entrySet()) {
            System.out.println("rowMap1-" + entry.getKey() + ":" + entry.getValue());
        }
        // 条件查询 family + row
        Map<String, String> rowMap2 = getValue(tableName, family, row2);
        System.out.println("查" + family + ":" + row2 + "=============================");
        for (Map.Entry<String, String> entry : rowMap2.entrySet()) {
            System.out.println("rowMap2-" + entry.getKey() + ":" + entry.getValue());
        }
        // 条件查询 family + row + column
        String dataValue = getValue(tableName, family, row1, "test_data2");
        System.out.println("查" + family + ": " + row2 + ": test_data2的值" + "=============================");
        System.out.println("data_value->" + dataValue);
        System.out.println("扫描全表=============================");
        // 查看表中所有数据
        getValue(tableName);

        if (true) {
            return;
        }

        // 删除行
        deleteRow(tableName, row2);
        // 查看表中所有数据
        System.out.println("删除行之后，扫描全表=============================");
        getValue(tableName);

        deleteFamily(tableName, family);
        System.out.println("删除列族 之后，扫描全表=============================");
        getValue(tableName);

    }

}
