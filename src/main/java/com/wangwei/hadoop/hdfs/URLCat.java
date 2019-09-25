package com.wangwei.hadoop.hdfs;

import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.io.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

/**
 * URL设置产生stream的工厂，注意URL是java自带的类
 * 设置工厂的方法是静态的，一旦某个应用设置了便不能修改
 * 通过web获取文件，故无需显式指定conf，即没必要知道元数据信息
 * 相当于发起的http请求，具体数据hadoop内置程序进行处理
 */
public class URLCat {
    static {
        URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
    }

    public static void main(String[] args) throws IOException {
        InputStream in;
        String url = "hdfs://localhost:9000/a.txt";
        in = new URL(url).openStream();
        IOUtils.copyBytes(in,System.out,4096,false);
        IOUtils.closeStream(in);
    }
}
