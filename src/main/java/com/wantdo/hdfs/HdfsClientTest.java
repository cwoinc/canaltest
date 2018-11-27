package com.wantdo.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IOUtils;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.net.URI;

/**
 * @author king
 * @version 2018-11-22 3:46 PM
 */
public class HdfsClientTest {
    
    private static final String HDFS_HOST = "hdfs://192.168.125.211:8020";
    private static final String HDFS_BASE_PATH = "/user/hdfs/king/";
    private static final String KING_BASE_PATH = "/home/king/diy/shared folders/";
    
    private FileSystem fs = null;
    
    @Before
    public void init() throws Exception {
        
        // 构造一个配置参数对象，设置一个参数：我们要访问的hdfs的URI
        // 从而FileSystem.get()方法就知道应该是去构造一个访问hdfs文件系统的客户端，以及hdfs的访问地址
        // new Configuration();的时候，它就会去加载jar包中的hdfs-default.xml
        // 然后再加载classpath下的hdfs-site.xml
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", HDFS_HOST);
        // 参数优先级： 1、客户端代码中设置的值 2、classpath下的用户自定义配置文件 3、然后是服务器的默认配置
        conf.set("dfs.replication", "3");
        
        // 获取一个hdfs的访问客户端，根据参数，这个实例应该是DistributedFileSystem的实例
        // fs = FileSystem.get(conf);
        
        //// 如果这样去获取，那conf里面就可以不要配"fs.defaultFS"参数，而且，这个客户端的身份标识已经是hadoop用户
        fs = FileSystem.get(new URI(HDFS_HOST), conf, "hdfs");
        
    }
    
    /**
     * 往hdfs上传文件
     */
    @Test
    public void testAddFileToHdfs() throws Exception {
        
        // 要上传的文件所在的本地路径
        Path src = new Path(KING_BASE_PATH + "kafka-2.1.0-src.tgz");
        // 要上传到hdfs的目标路径
        Path dst = new Path(HDFS_BASE_PATH + "test/aabb");
        fs.copyFromLocalFile(src, dst);
        fs.close();
    }
    
    /**
     * 从hdfs中复制文件到本地文件系统
     */
    @Test
    public void testDownloadFileToLocal() throws IllegalArgumentException, IOException {
        fs.copyToLocalFile(new Path(HDFS_BASE_PATH + "/deliver_fee_floder/CheckDeliverFee.hql"), new Path(KING_BASE_PATH + "aa"));
        fs.close();
    }
    
    @Test
    public void testMkdirAndDeleteAndRename() throws IllegalArgumentException, IOException {
        
        // 创建目录
        fs.mkdirs(new Path(HDFS_BASE_PATH + "test/a1/b1/c1"));
        
        // 删除文件夹 ，如果是非空文件夹，参数2必须给值true
        fs.delete(new Path(HDFS_BASE_PATH + "/aaa"), true);
        
        // 重命名文件或文件夹
        fs.rename(new Path(HDFS_BASE_PATH + "test/a1"), new Path(HDFS_BASE_PATH + "test/a2"));
        
    }
    
    /**
     * 查看目录信息，只显示文件
     */
    @Test
    public void testListFiles() throws FileNotFoundException, IllegalArgumentException, IOException {
        
        // 思考：为什么返回迭代器，而不是List之类的容器
        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path(HDFS_BASE_PATH), true);
        
        while (listFiles.hasNext()) {
            LocatedFileStatus fileStatus = listFiles.next();
            System.out.println(fileStatus.getPath().getName());
            System.out.println(fileStatus.getBlockSize());
            System.out.println(fileStatus.getPermission());
            System.out.println(fileStatus.getLen());
            BlockLocation[] blockLocations = fileStatus.getBlockLocations();
            for (BlockLocation bl : blockLocations) {
                System.out.println("block-length:" + bl.getLength() + "--" + "block-offset:" + bl.getOffset());
                String[] hosts = bl.getHosts();
                for (String host : hosts) {
                    System.out.println(host);
                }
            }
            System.out.println("--------------为angelababy打印的分割线--------------");
        }
    }
    
    /**
     * 查看文件及文件夹信息
     */
    @Test
    public void testListAll() throws FileNotFoundException, IllegalArgumentException, IOException {
        
        FileStatus[] listStatus = fs.listStatus(new Path(HDFS_BASE_PATH));
        
        String flag = "d--             ";
        for (FileStatus fstatus : listStatus) {
            if (fstatus.isFile()) {
                flag = "f--         ";
            }
            System.out.println(flag + fstatus.getPath().getName());
        }
    }
    
    /**
     * 通过流的方式上传文件到hdfs
     *
     * @throws Exception
     */
    @Test
    public void testUpload() throws Exception {
        
        FSDataOutputStream outputStream = fs.create(new Path(HDFS_BASE_PATH + "/bb"), true);
        FileInputStream inputStream = new FileInputStream(KING_BASE_PATH + "aa");
        
        org.apache.commons.io.IOUtils.copy(inputStream, outputStream);
        
    }
    
    @Test
    public void testDownLoadFileToLocal() throws IllegalArgumentException, IOException {
        
        //先获取一个文件的输入流----针对hdfs上的
        FSDataInputStream in = fs.open(new Path(HDFS_BASE_PATH + "bb"));
        
        //再构造一个文件的输出流----针对本地的
        FileOutputStream out = new FileOutputStream(new File(KING_BASE_PATH + "dd"));
        
        //再将输入流中数据传输到输出流
        IOUtils.copyBytes(in, out, 4096);
        
    }
    
    /**
     * hdfs支持随机定位进行文件读取，而且可以方便地读取指定长度
     * 用于上层分布式运算框架并发处理数据
     */
    @Test
    public void testRandomAccess() throws IllegalArgumentException, IOException {
        //先获取一个文件的输入流----针对hdfs上的
        FSDataInputStream in = fs.open(new Path(HDFS_BASE_PATH + "bb"));
        
        //可以将流的起始偏移量进行自定义
        in.seek(60);
        
        //再构造一个文件的输出流----针对本地的
        FileOutputStream out = new FileOutputStream(new File(KING_BASE_PATH + "cc"));
        
        IOUtils.copyBytes(in, out, 190L, true);
        
    }
    
    /**
     * 显示hdfs上文件的内容
     */
    @Test
    public void testCat() throws IllegalArgumentException, IOException {
        FSDataInputStream in = fs.open(new Path(HDFS_BASE_PATH + "/bb"));
        IOUtils.copyBytes(in, System.out, 1024);
    }
    
}
