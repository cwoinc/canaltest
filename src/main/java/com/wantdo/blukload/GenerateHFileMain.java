package com.wantdo.blukload;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author king
 * @version 2018-12-08 5:34 PM
 */
public class GenerateHFileMain {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        final String INPUT_PATH= "hdfs://master:9000/INFO/Input";
        final String OUTPUT_PATH= "hdfs://master:9000/HFILE/Output";
        Configuration conf = HBaseConfiguration.create();
        Connection connection = ConnectionFactory.createConnection(conf);
        Table table = connection.getTable(TableName.valueOf("TRAVEL"));
        Job job=Job.getInstance(conf);
        job.getConfiguration().set("mapred.jar", "/home/hadoop/TravelProject/out/artifacts/Travel/Travel.jar");  //预先将程序打包再将jar分发到集群上
        job.setJarByClass(GenerateHFileMain.class);
        job.setMapperClass(GenerateHFile.class);
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(Put.class);
        job.setOutputFormatClass(HFileOutputFormat2.class);
        HFileOutputFormat2.configureIncrementalLoad(job,table,connection.getRegionLocator(TableName.valueOf("TRAVEL")));
        FileInputFormat.addInputPath(job,new Path(INPUT_PATH));
        FileOutputFormat.setOutputPath(job,new Path(OUTPUT_PATH));
        System.exit(job.waitForCompletion(true)?0:1);
    }}
