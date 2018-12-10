package com.wantdo.blukload;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;
import org.apache.hadoop.hbase.mapreduce.KeyValueSortReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class TestHFileToHBase {
    
    public static class TestHFileToHBaseMapper extends Mapper {
        
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] values = value.toString().split(" ", 2);
            byte[] row = Bytes.toBytes(values[0]);
            ImmutableBytesWritable k = new ImmutableBytesWritable(row);
            KeyValue kvProtocol = new KeyValue(row, "PROTOCOLID".getBytes(), "PROTOCOLID".getBytes(), values[1]
                    .getBytes());
            context.write(k, kvProtocol);
        }
    }
    
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = HBaseConfiguration.create();
        Job job = new Job(conf, "TestHFileToHBase");
        job.setJarByClass(TestHFileToHBase.class);
        
        job.setOutputKeyClass(ImmutableBytesWritable.class);
        job.setOutputValueClass(KeyValue.class);
        
        job.setMapperClass(TestHFileToHBaseMapper.class);
        job.setReducerClass(KeyValueSortReducer.class);
//                job.setOutputFormatClass(org.apache.hadoop.hbase.mapreduce.HFileOutputFormat.class);
        job.setOutputFormatClass(HFileOutputFormat.class);
        // job.setNumReduceTasks(4);
        // job.setPartitionerClass(org.apache.hadoop.hbase.mapreduce.SimpleTotalOrderPartitioner.class);

//                 HBaseAdmin admin = new HBaseAdmin(conf);
        HTable table = new HTable(conf, "hua");
        
        HFileOutputFormat.configureIncrementalLoad(job, table);
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
    
}

