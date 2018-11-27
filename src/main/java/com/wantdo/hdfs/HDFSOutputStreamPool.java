package com.wantdo.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * 输出流池
 *
 * @author king
 * @version 2018-11-22 3:24 PM
 */
public class HDFSOutputStreamPool {
    private FileSystem fs;
    //存放所有的输出流
    private Map<String, FSDataOutputStream> pool = new HashMap<>();
    private static HDFSOutputStreamPool instance;
    
    private HDFSOutputStreamPool() {
        try {
            Configuration configuration = new Configuration();
            fs = FileSystem.get(configuration);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    //单例设计模式
    public static HDFSOutputStreamPool getInstance() {
        if (instance == null) {
            instance = new HDFSOutputStreamPool();
        }
        return instance;
    }
    
    public FSDataOutputStream getOutputStream(String path) {
        try {
            FSDataOutputStream out = pool.get(path);
            if (out == null) {
                Path p = new Path(path);
                if (!fs.exists(p)) { //若不存在就创建
                    out = fs.create(p);
                    pool.put(path, out);
                } else {
                    out = fs.append(p);//存在则直接追加
                    pool.put(path, out);
                }
            }
            return out;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }
}
