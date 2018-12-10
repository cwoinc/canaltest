package com.wantdo.blukload.use;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class HBaseHFileReducer extends Reducer<ImmutableBytesWritable, Text, ImmutableBytesWritable, KeyValue> {
    @Override
    protected void reduce(ImmutableBytesWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String value = "";
        while (values.iterator().hasNext()) {
            value = values.iterator().next().toString();
            if (value != null && !"".equals(value)) {
                KeyValue kv = createKeyValue(value);
                if (kv != null) {
                    context.write(key, kv);
                }
            }
        }
    }  // str格式为row:family:qualifier:value 简单模拟下
    
    private KeyValue createKeyValue(String str) {
        String[] strs = str.split(":");
        if (strs.length < 4) {
            return null;
        }
        String row = strs[0];
        String family = strs[1];
        String qualifier = strs[2];
        String value = strs[3];
        return new KeyValue(Bytes.toBytes(row), Bytes.toBytes(family), Bytes.toBytes(qualifier), System.currentTimeMillis(), Bytes.toBytes(value));
    }
}
