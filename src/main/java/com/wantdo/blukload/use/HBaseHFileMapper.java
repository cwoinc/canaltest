package com.wantdo.blukload.use;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class HBaseHFileMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Text> {
    private ImmutableBytesWritable immutableBytesWritable = new ImmutableBytesWritable();
    
    @Override
    protected void map(LongWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException {
        immutableBytesWritable.set(Bytes.toBytes(key.get()));
        context.write(immutableBytesWritable, value);
    }
}
