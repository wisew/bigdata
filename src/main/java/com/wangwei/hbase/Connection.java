package com.wangwei.hbase;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;

public class Connection {
    public static void main(String[] args) throws IOException {
        // 创建hbase连接
        Configuration conf = HBaseConfiguration.create();
        org.apache.hadoop.hbase.client.Connection connection = ConnectionFactory.createConnection(conf);
        // 获取操作对象
        Admin admin = connection.getAdmin();
        // 判断命名空间
        try{
            NamespaceDescriptor ns = admin.getNamespaceDescriptor("xxx");
        } catch (NamespaceNotFoundException e){
            // 没有该命名空间,创建
            NamespaceDescriptor nd = NamespaceDescriptor.create("xxx").build();
            admin.createNamespace(nd);
        }
        boolean b = admin.tableExists(TableName.valueOf("xxx:student"));
        System.out.println(b);
        if (!b){
            // 添加列族
            ColumnFamilyDescriptor info = ColumnFamilyDescriptorBuilder.newBuilder(ColumnFamilyDescriptorBuilder.of("info")).build();
            // 创建表
            TableDescriptor table = TableDescriptorBuilder.newBuilder(TableName.valueOf("xxx:student")).setColumnFamily(info).build();
            admin.createTable(table);
        }
        System.out.println(admin.tableExists(TableName.valueOf("xxx:student")));
        connection.close();
    }
}

