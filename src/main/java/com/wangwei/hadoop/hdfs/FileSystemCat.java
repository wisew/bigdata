package com.wangwei.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.fs.FileSystem;

import java.io.*;
import java.net.URI;

public class FileSystemCat {
    public static void main(String[] args) throws IOException {
        String from = args[0];
        String to = args[1];
        writeByFs(from, to);
        readByFs(to);
    }

    /**
     * 通过FileSystem读取数据
     * 需要显式指定conf，因为需要知道元数据信息
     * fs.open返回的是FSDataInputStream，可以指定seek位置
     * 跳过多少，得转成stream来考虑，如一个UTF-8字符占用3
     */
    private static void readByFs(String arg) throws IOException {
        String uri = arg;
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(uri), conf);
        InputStream in = null;
        in = fs.open(new Path(uri));
        BufferedOutputStream out = new BufferedOutputStream(new FileOutputStream("/root/spark_app/c.txt"));
        try {
            IOUtils.copyBytes(in, out, 1024 * 4, false);
            ((FSDataInputStream) in).seek(6);
            IOUtils.copyBytes(in, System.out, 1024 * 4, false);
        } finally {
            IOUtils.closeStream(in);
            IOUtils.closeStream(out);
        }
    }

    /**
     * 通过FileSystem读取数据
     * 创建写入流FSDataOutputStream可以指定很多参数，例如是否覆盖，打印上传进度等
     * 本质其实是IO流+网络编程
     * @param from
     * @param to
     */
    private static void writeByFs(String from, String to) {
        BufferedInputStream bis = null;
        FSDataOutputStream fdos = null;

        try {
            bis = new BufferedInputStream(new FileInputStream(from));
            Configuration conf = new Configuration();
            FileSystem fs = FileSystem.get(URI.create(to), conf);
            fdos = fs.create(new Path(to), true,1024*4,() -> System.out.print("."));
            IOUtils.copyBytes(bis, fdos, 1024 * 4, false);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (fdos != null) {
                IOUtils.closeStream(fdos);
            }
            if (bis != null) {
                IOUtils.closeStream(bis);
            }
        }
    }
}
