package com.wantdo.blukload;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

/*
 * adminOnOff.log 文件格式：
 * topsid   uid   roler_num   typ   time
 * */
public class HFileImportMapper2 extends
        Mapper<LongWritable, Text, ImmutableBytesWritable, KeyValue> {
    protected SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
    protected final String CF_KQ = "kq";//考勤
    protected final int ONE = 1;
    
    @Override
    protected void map(LongWritable key, Text value, Mapper.Context context)
            throws IOException, InterruptedException {
        String line = value.toString();
        System.out.println("line : " + line);
        String[] datas = line.split("\\s+");
        // row格式为:yyyyMMdd-sid-uid-role_num-timestamp-typ
        String row = sdf.format(new Date(Long.parseLong(datas[4])))
                + "-" + datas[0] + "-" + datas[1] + "-" + datas[2]
                + "-" + datas[4] + "-" + datas[3];
        ImmutableBytesWritable rowkey = new ImmutableBytesWritable(
                Bytes.toBytes(row));
        KeyValue kv = new KeyValue(Bytes.toBytes(row), this.CF_KQ.getBytes(), datas[3].getBytes(), Bytes.toBytes(this.ONE));
        context.write(rowkey, kv);
    }
}