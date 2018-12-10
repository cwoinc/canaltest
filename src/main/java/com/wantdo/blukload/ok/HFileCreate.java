package com.wantdo.blukload.ok;

import com.google.common.collect.Maps;
import com.wantdo.demo.util.HBaseConnectionUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

/**
 * mr生成HFile, 导入Hbase表中（适用于空表，数据较大,只有一个列族的表）
 *
 * @author gy
 * @date 2018年7月18日 上午9:18:22
 * 生成Hfile以后执行： hbase org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles outputpath tablename
 */
public class HFileCreate {
    
    private static String[] heads;
    
    private static final String KEY_NAME = "id";
    
    public static class HFileCreateMap extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {
        
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] valueStrSplit = value.toString().split("\t");
            if (null == heads) {
                heads = valueStrSplit;
                return;
            }
            if (heads.length < valueStrSplit.length) {
                throw new RuntimeException("长度错误：" + Arrays.toString(heads) + "\t" + Arrays.toString(valueStrSplit));
            }
            
            String keyValue = null;
            int len = valueStrSplit.length;
            Map<String, String> map = Maps.newHashMap();
            for (int i = 0; i < len; i++) {
                String column = heads[i];
                String val = valueStrSplit[i];
                if (KEY_NAME.equals(column)) {
                    keyValue = val;
                }
                map.put(column, val);
            }
            
            if (StringUtils.isBlank(keyValue)) {
                throw new RuntimeException("rowKey不能为空");
            }
            byte[] rowKey = Bytes.toBytes(keyValue);
            ImmutableBytesWritable rowKeyWritable = new ImmutableBytesWritable(rowKey);
            Put put = new Put(rowKey);
            map.forEach((column, val) -> put.addColumn(Bytes.toBytes("i"), Bytes.toBytes(column), Bytes.toBytes(val)));
            context.write(rowKeyWritable, put);
        }
    }
    
    public static void main(String[] args) throws Exception {
        Configuration conf = HBaseConnectionUtil.getConfiguration();
        Connection conn = ConnectionFactory.createConnection(conf);
        
        Table table = conn.getTable(TableName.valueOf(HBaseConnectionUtil.TABLE_NAME));
        try {
            Job job = Job.getInstance(conf, "ExampleRead");
            job.setJarByClass(HFileCreate.class);
            job.setMapperClass(HFileCreate.HFileCreateMap.class);
            job.setMapOutputKeyClass(ImmutableBytesWritable.class);
            // 要和extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put>里的最后一个对象对上
            job.setMapOutputValueClass(Put.class);
            // speculation
            job.setSpeculativeExecution(false);
            job.setReduceSpeculativeExecution(false);
            // in/out format
            job.setInputFormatClass(TextInputFormat.class);
            job.setOutputFormatClass(HFileOutputFormat2.class);
            
            FileInputFormat.setInputPaths(job, HBaseConnectionUtil.INPUT_PATH);
            FileOutputFormat.setOutputPath(job, new Path(HBaseConnectionUtil.OUTPUT_PATH));
            HFileOutputFormat2.configureIncrementalLoad(job, table, ((HTable) table).getRegionLocator());
            try {
                job.waitForCompletion(true);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (table != null) {
                table.close();
            }
            conn.close();
        }
    }
}