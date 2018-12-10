package com.wantdo.blukload.ok;

import com.wantdo.demo.util.HBaseConnectionUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;

/**
 * @author king
 * @version 2018-12-08 5:35 PM
 */
public class LoadIncrementalHFileToHBase {
    public static void main(String[] args) throws Exception {
        Connection connection = HBaseConnectionUtil.getHBaseConnection();
        Admin admin = connection.getAdmin();
        TableName travel = TableName.valueOf(HBaseConnectionUtil.TABLE_NAME);
        Table table = connection.getTable(travel);
        LoadIncrementalHFiles load = new LoadIncrementalHFiles(HBaseConnectionUtil.getConfiguration());
        load.doBulkLoad(new Path(HBaseConnectionUtil.BASE_PATH + "1544411373887"), admin, table, connection.getRegionLocator(travel));
    }
}
