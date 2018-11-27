package com.wantdo.hdfs;

import org.apache.hadoop.fs.FSDataOutputStream;

import java.io.IOException;

/**
 * hdfs写入器
 *
 * @author king
 * @version 2018-11-22 3:23 PM
 */
public class HDFSWriter {
    
    /*
    写入log到hdfs文件
    hdfs://mycluster/eshop/2018/07/18/s128.log | s129.log | s130.log
     */
    public void writeLog2HDFS(String path, byte[] log) {
        try {
            FSDataOutputStream outputStream = HDFSOutputStreamPool.getInstance().getOutputStream(path);
            outputStream.write(log);
            outputStream.write("\r\n".getBytes());
            outputStream.hsync();
            outputStream.hflush();
        } catch (IOException e) {
            e.printStackTrace();
        }

//        FSDataInputStream in = fileSystem.open(new Path("hdfs://mycluster/user/centos/words.txt"));
//        ByteArrayOutputStream baos = new ByteArrayOutputStream();
//        IOUtils.copyBytes(in,baos,1024);
//        System.out.println(new String(baos.toByteArray()));
    }
    
}