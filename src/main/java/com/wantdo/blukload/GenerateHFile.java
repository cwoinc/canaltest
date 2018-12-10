package com.wantdo.blukload;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author king
 * @version 2018-12-08 5:33 PM
 */
public class GenerateHFile extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {
    
    @Override
    protected void map(LongWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] items = line.split("\t");
        
        String ROWKEY = items[1] + items[2] + items[3];
        ImmutableBytesWritable rowkey = new ImmutableBytesWritable(ROWKEY.getBytes());
        Put put = new Put(ROWKEY.getBytes());   //ROWKEY
        put.addColumn("INFO".getBytes(), "URL".getBytes(), items[0].getBytes());
        put.addColumn("INFO".getBytes(), "SP".getBytes(), items[1].getBytes());  //出发点
        put.addColumn("INFO".getBytes(), "EP".getBytes(), items[2].getBytes());  //目的地
        put.addColumn("INFO".getBytes(), "ST".getBytes(), items[3].getBytes());   //出发时间
        put.addColumn("INFO".getBytes(), "PRICE".getBytes(), Bytes.toBytes(Integer.valueOf(items[4])));  //价格
        put.addColumn("INFO".getBytes(), "TRAFFIC".getBytes(), items[5].getBytes());//交通方式
        put.addColumn("INFO".getBytes(), "HOTEL".getBytes(), items[6].getBytes()); //酒店
        
        context.write(rowkey, put);
    }
}
