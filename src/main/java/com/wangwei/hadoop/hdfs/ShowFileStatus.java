package com.wangwei.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;

import java.io.IOException;
import java.net.URI;


public class ShowFileStatus {
    private FileSystem fs;
    public void setup() throws IOException {
        Configuration conf = new Configuration();
        fs = FileSystem.get(URI.create("/dir/file"),conf);
        FSDataOutputStream fsDataOutputStream = fs.create(new Path("/Users/wangwei/hadoop_test_file/file"),true);
        fsDataOutputStream.write("content".getBytes());
        IOUtils.closeStream(fsDataOutputStream);
    }

    public void fileStatusForFile() throws IOException {
        Path file = new Path("/Users/wangwei/hadoop_test_file/file");
        FileStatus stat = fs.getFileStatus(file);
        System.out.println("hadoop的资源路径:"+stat.getPath().toString());
        System.out.println("统一资源定位符:"+stat.getPath().toUri().getPath());
        System.out.println("是否是目录:"+stat.isDirectory());
        System.out.println("文件大小Byte:"+stat.getLen());
        System.out.println("文件修改时间:"+stat.getModificationTime());
        System.out.println("文件所有者:"+stat.getOwner());
        System.out.println("文件所有组:"+stat.getGroup());
        System.out.println("副本个数:"+stat.getReplication());
        System.out.println("创建时间:"+stat.getAccessTime());
        System.out.println("权限信息:"+stat.getPermission().toString());
        System.out.println("文件块大小:"+stat.getBlockSize());
        System.out.println("文件名:"+stat.getPath().getName());
    }

    public static void main(String[] args) throws IOException {
        ShowFileStatus sfs = new ShowFileStatus();
        sfs.setup();
        sfs.fileStatusForFile();
    }
}
