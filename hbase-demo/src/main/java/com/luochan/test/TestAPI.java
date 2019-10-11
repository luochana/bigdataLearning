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
    public static void createTable(String tableName,String... cfs) throws IOException {

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
        admin.createTable(hTableDescriptor);

    }

    //3.删除表
    public static void dropTable(String tableName) throws IOException {
        if(!isTableExist(tableName)){
            System.out.println(tableName+"表不存在!");
            return;
        }

        admin.disableTable(TableName.valueOf(tableName));
        admin.deleteTable(TableName.valueOf(tableName));

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
    public static void putData(String tableName,String rowKey,String cf,String cn,String value) throws IOException {

        Table table = connection.getTable(TableName.valueOf(tableName));
        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes(cf),Bytes.toBytes(cn),Bytes.toBytes(value));
        table.put(put);
        table.close();

    }

    //6.获取数据,get()

    public static void getData(String tableName,String rowKey,String cf,String cn) throws IOException {
        if(!isTableExist(tableName)){
            System.out.println(tableName+"表不存在!");
            return;
        }
        Table table = connection.getTable(TableName.valueOf(tableName));
        Get get = new Get(Bytes.toBytes(rowKey));

        //指定获取的列族
        // get.addFamily(Bytes.toBytes(cf));

        //指定列族和列
        //get.addColumn(Bytes.toBytes(cf),Bytes.toBytes(cn));

        Result result = table.get(get);

        for (Cell cell : result.rawCells()) {
            System.out.println("列族:"+CellUtil.cloneFamily(cell)+",列名:"+CellUtil.cloneQualifier(cell)+",值:"+CellUtil.cloneValue(cell));
        }
        table.close();
    }

    //7.获取数据scan
    public static void scanTable(String tableName) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();

        //扫描指定rowKey
     //   Scan scan1 = new Scan().withStartRow(Bytes.toBytes("1001")).withStopRow(Bytes.toBytes("1003"));
        ResultScanner tableScanner = table.getScanner(scan);

        for (Result result : tableScanner) {
            for (Cell cell : result.rawCells()) {
                System.out.println("列族:"+CellUtil.cloneFamily(cell)+",列名:"+CellUtil.cloneQualifier(cell)+",值:"+CellUtil.cloneValue(cell));
            }
        }
        table.close();
    }


    //8.删除数据
    public static void deleteData(String tableName,String rowKey,String cf,String cn) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));

        //删除指定rowKey的数据.打上deleteFamily标记
        Delete delete = new Delete(Bytes.toBytes(rowKey));

        //删除所有版本,对列打上deleteColumn标记
        //delete.addColumns(Bytes.toBytes(cf),Bytes.toBytes(cn));
        //删除最新版本数据, 对最新数据打上Delete标记, 如果指定时间戳,就对指定时间戳打上delete标记,对其他时间戳的数据没有影响
        //delete.addColumn(Bytes.toBytes(cf),Bytes.toBytes(cn));
        //删除指定列族,打上deleteFamily标记.
        //delete.addFamily(Bytes.toBytes(cf));

        table.delete(delete);
        table.close();
    }

    public static void main(String[] args) throws IOException {
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
     //   putData("student","1003","info","name","lisi");

        //6.获取数据,get()
      //  getData("student","1003","info","name");


        //7.获取数据scan
      //  scanTable("student");

        //8.删除数据
    //    deleteData("student","1001","info","name");


     //   System.out.println(isTableExist("stu1"));

        close();
    }
}
