package com.wangwei.hadoop.hdfs;

import org.apache.commons.crypto.utils.IoUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

/**
 * LocalFileSystem默认自带校验和，可以手动设置取消
 * RawLocalFileSystem不带校验和
 * 获取FileSystem有两种方式：
 * 1、通过FileSystem的静态方法
 * 2、new实例，然后initialize
 */
public class ChecksumSet {
    public static void main(String[] args) throws IOException {
        Configuration conf = new Configuration();
        Path path = new Path("/Users/wangwei/hadoop_test_file/checkFile");
//        withoutCheckSumWrite(conf, path);
//        checkSumWrite(conf, path);
        checkSumRead(conf, path);
//        withoutChecksumRead(conf,path);
    }

    /**
     * @param conf
     * @param path
     * @throws IOException
     */
    public static void checkSumRead(Configuration conf, Path path) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        // 设置不校验
        // fs.setVerifyChecksum(false);
        InputStream inputStream = commonRead(fs, path);
        IOUtils.copyBytes(inputStream, System.out, 4 * 1024, false);
        IOUtils.closeStream(inputStream);
    }

    public static void withoutChecksumRead(Configuration conf, Path path) throws IOException {
        RawLocalFileSystem rawFs = new RawLocalFileSystem();
        rawFs.initialize(URI.create("/"), conf);
        InputStream inputStream = commonRead(rawFs, path);
        IOUtils.copyBytes(inputStream, System.out, 4 * 1024, false);
        IOUtils.closeStream(inputStream);
    }

    /**
     * FileSystem.get返回一个LocalFileSystem实例
     * 写入或者读取都会进行CRC校验
     */
    public static void checkSumWrite(Configuration conf, Path path) {
        try {
            FileSystem fs = FileSystem.get(conf);
            System.out.println(fs);
            commonWrite(fs, path);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 使用RawLocalFileSystem禁用校验和
     *
     * @param conf
     * @param path
     */
    public static void withoutCheckSumWrite(Configuration conf, Path path) {
        try {
            RawLocalFileSystem rawFs = new RawLocalFileSystem();
            rawFs.initialize(URI.create("/"), conf);
            commonWrite(rawFs, path);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static InputStream commonRead(FileSystem fs, Path path) throws IOException {
        return fs.open(path);
    }

    /**
     * FileSystem调用create获取一个输出流写入数据
     *
     * @param fs
     * @param path
     * @throws IOException
     */
    public static void commonWrite(FileSystem fs, Path path) throws IOException {
        FSDataOutputStream fsDataOutputStream = fs.create(path, true);
        fsDataOutputStream.write("contents".getBytes());
        IOUtils.closeStream(fsDataOutputStream);
    }
}
