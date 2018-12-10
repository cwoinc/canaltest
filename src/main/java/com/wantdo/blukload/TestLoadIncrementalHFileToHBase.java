package com.wantdo.blukload;

import com.wantdo.demo.util.HBaseConnectionUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;

public class TestLoadIncrementalHFileToHBase {
    
    public static void main(String[] args) throws Exception {
        //Configuration conf = HBaseConfiguration.create();
        //String[] dfsArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    
        Configuration conf = HBaseConnectionUtil.getConfiguration();
        HTable table = new HTable(conf,  "wantdo:az_mws.fe_charge_component");
        LoadIncrementalHFiles loader = new LoadIncrementalHFiles(conf);
        loader.doBulkLoad(new Path("/home/king/diy/shared_folders/1544406779230"), table);
    }
}
