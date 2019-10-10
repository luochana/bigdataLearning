package com.luochan.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;


import java.io.IOException;

public class TestAPI {

    private static Connection connection=null;
    private static Admin admin=null;

    static{
        try {
            Configuration configuration = HBaseConfiguration.create();
            configuration.set("hbase.zookeeper.quorum","192.168.56.101");
            connection= ConnectionFactory.createConnection(configuration);
            admin=connection.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void close(){
        if(admin!=null){
            try {
                admin.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        if(connection!=null){
            try {
                connection.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    //1.测试表是否存在
    public static boolean isTableExist(String tableName){
        try {
            return admin.tableExists(TableName.valueOf(tableName));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }


    //2.创建表
    public static void createTable(String tableName,String... cfs){

        if(cfs.length==0){
            System.out.println("请设置列族");
            return;
        }

        if(isTableExist(tableName)){
            System.out.println(tableName+"表已存在!");
            return;
        }

        HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));

        for (String cf : cfs) {
            hTableDescriptor.addFamily(new HColumnDescriptor(cf));
        }
        try {
            admin.createTable(hTableDescriptor);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    //3.删除表
    public static void dropTable(String tableName){
        if(!isTableExist(tableName)){
            System.out.println(tableName+"表不存在!");
            return;
        }

        try {
            admin.disableTable(TableName.valueOf(tableName));
            admin.deleteTable(TableName.valueOf(tableName));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    //4.创建命名空间
    public static void createNameSpace(String ns){

        NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create(ns).build();
        try {
            admin.createNamespace(namespaceDescriptor);
        }catch (NamespaceExistException e){
            System.out.println(ns+":命名空间已存在!");
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }

    //5.向表中插入数据
    public static void putData(String tableName,String rowKey,String cf,String cn,String value) {

        Table table = null;
        try {
            table = connection.getTable(TableName.valueOf(tableName));
            Put put = new Put(Bytes.toBytes(rowKey));
            put.addColumn(Bytes.toBytes(cf),Bytes.toBytes(cn),Bytes.toBytes(value));
            table.put(put);
            table.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }



    public static void main(String[] args) {
        //1.测试表是否存在
     //   System.out.println(isTableExist("stu1"));

        //2.创建表
     //   createTable("stu1","info");

        //3.删除表
     //   dropTable("stu1");

        //4.创建命名空间
    //    createNameSpace("bigData");
    //    createTable("bigData:tb1","info");

        //5.向表中插入数据
        putData("student","1003","info","name","lisi");

     //   System.out.println(isTableExist("stu1"));

        close();
    }
}
