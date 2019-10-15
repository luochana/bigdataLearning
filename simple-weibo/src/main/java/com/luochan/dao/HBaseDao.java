package com.luochan.dao;

import com.luochan.constants.Constants;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class HBaseDao {
    public static void publishWeiBo(String uid,String content) throws IOException {

        //插入微博内容
        Connection connection = ConnectionFactory.createConnection(Constants.CONFIGURATION);
        Table contentTable = connection.getTable(TableName.valueOf(Constants.CONTENT_TABLE));
        long ts = System.currentTimeMillis();
        String rowKey=uid+"_"+ts;
        Put contentPut = new Put(Bytes.toBytes(rowKey));
        contentPut.addColumn(Bytes.toBytes(Constants.CONTENT_TABLE_CF),Bytes.toBytes("content"),Bytes.toBytes(content));
        contentTable.put(contentPut);

        //操作用户关系表

        Table relationTable = connection.getTable(TableName.valueOf(Constants.RELATION_TABLE));
        Get get = new Get(Bytes.toBytes(uid));
        get.addFamily(Bytes.toBytes(Constants.RELATION_TABLE_CF2));
        Result result = relationTable.get(get);
        ArrayList<Put> inboxPuts = new ArrayList<Put>();
        for (Cell cell : result.rawCells()) {
            Put inboxPut = new Put(CellUtil.cloneQualifier(cell));
            inboxPut.addColumn(Bytes.toBytes(Constants.INBOX_TABLE_CF),Bytes.toBytes(uid),Bytes.toBytes(rowKey));
            inboxPuts.add(inboxPut);
        }

        if(inboxPuts.size()>0){
            Table inboxTable = connection.getTable(TableName.valueOf(Constants.INBOX_TABLE));
            inboxTable.put(inboxPuts);
            inboxTable.close();
        }

        relationTable.close();
        contentTable.close();
        connection.close();
    }

    public static void addAttends(String uid,String... attends) throws IOException {
        if(attends.length<=0){
            System.out.println("请输入待关注的人!");
            return;
        }

        //操作用户关系表
        Connection connection = ConnectionFactory.createConnection(Constants.CONFIGURATION);
        Table relationTable = connection.getTable(TableName.valueOf(Constants.CONTENT_TABLE));
        ArrayList<Put> relationPuts = new ArrayList<Put>();
        Put uidPut = new Put(Bytes.toBytes(uid));
        for (String attend : attends) {
            uidPut.addColumn(Bytes.toBytes(Constants.RELATION_TABLE_CF1),Bytes.toBytes(attend),Bytes.toBytes(attend));
            Put attendPut = new Put(Bytes.toBytes(attend));
            attendPut.addColumn(Bytes.toBytes(Constants.RELATION_TABLE_CF2),Bytes.toBytes(uid),Bytes.toBytes(uid));
            relationPuts.add(attendPut);
        }
        relationPuts.add(uidPut);
        relationTable.put(relationPuts);


        //操作收件箱表
        Table contentTable = connection.getTable(TableName.valueOf(Constants.CONTENT_TABLE));
        Put inboxPut = new Put(Bytes.toBytes(uid));
        for (String attend : attends) {
            Scan scan = new Scan().withStartRow(Bytes.toBytes(attend + "_")).withStopRow(Bytes.toBytes(attend + "|"));
            ResultScanner resultScanner = contentTable.getScanner(scan);

            long ts = System.currentTimeMillis();
            for (Result result : resultScanner) {
                inboxPut.addColumn(Bytes.toBytes(Constants.INBOX_TABLE_CF),Bytes.toBytes(attend),ts++,result.getRow());
            }
        }

        if(!inboxPut.isEmpty()){
            Table inboxTable = connection.getTable(TableName.valueOf(Constants.INBOX_TABLE));
            inboxTable.put(inboxPut);
            inboxTable.close();

        }

        relationTable.close();
        connection.close();
        connection.close();
    }

    //取消关注
    public static void deleteAttends(String uid,String... dels) throws IOException {
        Connection connection = ConnectionFactory.createConnection(Constants.CONFIGURATION);

        //操作用户关系表
        Table relationTable = connection.getTable(TableName.valueOf(Constants.RELATION_TABLE));
        List<Delete> deleteList = new ArrayList<Delete>();
        Delete uidDelete = new Delete(Bytes.toBytes(uid));

        for (String del : dels) {
            uidDelete.addColumns(Bytes.toBytes(Constants.RELATION_TABLE_CF1),Bytes.toBytes(del));
            Delete delDelete = new Delete(Bytes.toBytes(del));
            delDelete.addColumns(Bytes.toBytes(Constants.RELATION_TABLE_CF2), Bytes.toBytes(uid));
            deleteList.add(delDelete);
        }

        deleteList.add(uidDelete);
        relationTable.delete(deleteList);

        //操作收件箱表
        Table inboxTable = connection.getTable(TableName.valueOf(Constants.INBOX_TABLE));
        Delete inboxDelete = new Delete(Bytes.toBytes(uid));
        for (String del : dels) {
            inboxDelete.addColumns(Bytes.toBytes(Constants.INBOX_TABLE_CF), Bytes.toBytes(del));
        }

        inboxTable.delete(inboxDelete);

        relationTable.close();
        inboxTable.close();
        connection.close();
    }

    //获取初始化页面
    public static void getInit(String uid) throws IOException {
        Connection connection = ConnectionFactory.createConnection(Constants.CONFIGURATION);
        Table inboxTable = connection.getTable(TableName.valueOf(Constants.INBOX_TABLE));
        Table contentTable = connection.getTable(TableName.valueOf(Constants.CONTENT_TABLE));
        Get inboxGet = new Get(Bytes.toBytes(uid));
        inboxGet.setMaxVersions();
        Result result = inboxTable.get(inboxGet);
        for (Cell cell : result.rawCells()) {
            Get contentGet = new Get(CellUtil.cloneValue(cell));
            Result contResult = contentTable.get(contentGet);

            for (Cell contentCell : contResult.rawCells()) {
                System.out.println("PK:"+Bytes.toString(CellUtil.cloneRow(contentCell))+
                        "CF:"+Bytes.toString(CellUtil.cloneFamily(contentCell))+
                        "CN:"+Bytes.toString(CellUtil.cloneQualifier(contentCell))+
                        "value:"+Bytes.toString(CellUtil.cloneValue(contentCell)));
            }
        }

        inboxTable.close();
        contentTable.close();
        connection.close();
    }

    //获取某个人的苏所有微博
    public static void getWeiBo(String uid) throws IOException {
        Connection connection = ConnectionFactory.createConnection(Constants.CONFIGURATION);
        Table contentTable = connection.getTable(TableName.valueOf(Constants.CONTENT_TABLE));
        Scan scan = new Scan();

        RowFilter rowFilter = new RowFilter(CompareFilter.CompareOp.EQUAL, new SubstringComparator(uid + "_"));
        scan.setFilter(rowFilter);

        ResultScanner resultScanner = contentTable.getScanner(scan);

        for (Result result : resultScanner) {
            for (Cell cell : result.rawCells()) {
                System.out.println("PK:"+Bytes.toString(CellUtil.cloneRow(cell))+
                        "CF:"+Bytes.toString(CellUtil.cloneFamily(cell))+
                        "CN:"+Bytes.toString(CellUtil.cloneQualifier(cell))+
                        "value:"+Bytes.toString(CellUtil.cloneValue(cell)));
            }
        }
        contentTable.close();
        connection.close();
    }
}
