package com.wantdo.blukload.use;

import com.wantdo.demo.util.HBaseConnectionUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author jinxl
 */
public class HbaseHFileDriver {
    
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Job job = Job.getInstance();
        job.setJarByClass(HbaseHFileDriver.class);
        
        job.setMapperClass(HBaseHFileMapper.class);
        job.setReducerClass(HBaseHFileReducer.class);
        
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(Text.class);
        // 偷懒， 直接写死在程序里了，实际应用中不能这样, 应从命令行获取
        FileInputFormat.addInputPath(job, new Path(HBaseConnectionUtil.INPUT_PATH));
        FileOutputFormat.setOutputPath(job, new Path(HBaseConnectionUtil.OUTPUT_PATH));
        
        Connection connection = HBaseConnectionUtil.getHBaseConnection();
        
        Table table = connection.getTable(TableName.valueOf(HBaseConnectionUtil.TABLE_NAME));
        HFileOutputFormat2.configureIncrementalLoadMap(job, table);
        
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}