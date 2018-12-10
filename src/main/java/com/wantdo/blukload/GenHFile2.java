package com.wantdo.blukload;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;

public class GenHFile2 {
    
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.addResource("myConf.xml");
        String input = conf.get("input");
        String output = conf.get("output");
        String tableName = conf.get("source_table");
        System.out.println("table : " + tableName);
        
        HTable table;
        try {
            //运行前，删除已存在的中间输出目录
            try {
                FileSystem fs = FileSystem.get(URI.create(output), conf);
                fs.delete(new Path(output), true);
                fs.close();
            } catch (IOException e1) {
                e1.printStackTrace();
            }
            
            table = new HTable(conf, tableName.getBytes());
            Job job = new Job(conf);
            job.setJobName("Generate HFile");
            
            job.setJarByClass(HFileImportMapper2.class);
            job.setInputFormatClass(TextInputFormat.class);
            job.setMapperClass(HFileImportMapper2.class);
            FileInputFormat.setInputPaths(job, input);

//job.setReducerClass(KeyValueSortReducer.class);
//job.setMapOutputKeyClass(ImmutableBytesWritable.class);
//job.setMapOutputValueClass(KeyValue.class);
            job.getConfiguration().set("mapred.mapoutput.key.class", "org.apache.hadoop.hbase.io.ImmutableBytesWritable");
            job.getConfiguration().set("mapred.mapoutput.value.class", "org.apache.hadoop.hbase.KeyValue");

//job.setOutputFormatClass(HFileOutputFormat.class);
            FileOutputFormat.setOutputPath(job, new Path(output));
            //job.setPartitionerClass(SimpleTotalOrderPartitioner.class);
            HFileOutputFormat.configureIncrementalLoad(job, table);
            try {
                job.waitForCompletion(true);
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}