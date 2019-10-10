package com.luochan.hdfs;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class HDFSClient {

    @Test
    public void createDir() throws URISyntaxException, IOException, InterruptedException {
        Configuration conf = new Configuration();
//		conf.set("fs.defaultFS", "hdfs://hadoop102:9000");

        // 1 获取hdfs客户端对象
//		FileSystem fs = FileSystem.get(conf );
        FileSystem fs = FileSystem.get(new URI("hdfs://spark1:9000"), conf, "luochan");

        // 2 在hdfs上创建路径
        fs.mkdirs(new Path("/TIM"));

        // 3 关闭资源
        fs.close();

        System.out.println("over");
    }

    @Test
    public void testListFiles() throws IOException, InterruptedException, URISyntaxException {
        // 1 获取对象
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://spark1:9000"), conf , "luochan");

        // 2 查看文件详情
        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);

        while(listFiles.hasNext()){
            LocatedFileStatus fileStatus = listFiles.next();

            // 查看文件名称、权限、长度、块信息
            System.out.println(fileStatus.getPath().getName());// 文件名称
            System.out.println(fileStatus.getPermission());// 文件权限
            System.out.println(fileStatus.getLen());// 文件长度

            BlockLocation[] blockLocations = fileStatus.getBlockLocations();

            for (BlockLocation blockLocation : blockLocations) {

                String[] hosts = blockLocation.getHosts();

                for (String host : hosts) {
                    System.out.println(host);
                }
            }

            System.out.println("------分割线--------");
        }

        // 3 关闭资源
        fs.close();
    }


    @Test
    public void testCopyFromLocalFile() throws IOException, InterruptedException, URISyntaxException{

        // 1 获取fs对象
        Configuration conf = new Configuration();
       // conf.set("dfs.replication", "2");
        FileSystem fs = FileSystem.get(new URI("hdfs://spark1:9000"), conf , "luochan");

        // 2 执行上传API
        fs.copyFromLocalFile(new Path("/home/luochan/TIM"), new Path("/"));

        // 3 关闭资源
        fs.close();
    }


    @Test
    public void testCopyToLocalFile() throws IOException, InterruptedException, URISyntaxException{

        // 1 获取对象
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://spark1:9000"), conf , "luochan");

        // 2 执行下载操作
//		fs.copyToLocalFile(new Path("/banhua.txt"), new Path("e:/banhua.txt"));
        fs.copyToLocalFile(false, new Path("/TIM/test.txt"), new Path("/home/luochan/dfsFliesDir/TIM"), true);

        // 3 关闭资源
        fs.close();
    }

    @Test
    public void testDelete() throws IOException, InterruptedException, URISyntaxException{

        // 1 获取对象
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://spark1:9000"), conf , "luochan");

        // 2 文件删除
        fs.delete(new Path("/TIM"), true);

        // 3 关闭资源
        fs.close();
    }

    @Test
    public void testRename() throws IOException, InterruptedException, URISyntaxException{

        // 1 获取对象
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://spark1:9000"), conf , "luochan");

        // 2 执行更名操作
        fs.rename(new Path("/TIM"), new Path("/TIM2"));

        // 3 关闭资源
        fs.close();
    }

    // 6 判断是文件还是文件夹
    @Test
    public void testListStatus() throws IOException, InterruptedException, URISyntaxException{

        // 1 获取对象
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://spark1:9000"), conf , "luochan");

        // 2 判断操作
        FileStatus[] listStatus = fs.listStatus(new Path("/"));

        for (FileStatus fileStatus : listStatus) {

            if (fileStatus.isFile()) {
                // 文件
                System.out.println("f:"+fileStatus.getPath().getName());
            }else{
                // 文件夹
                System.out.println("d:"+fileStatus.getPath().getName());
            }
        }

        // 3 关闭资源
        fs.close();
    }

    @Test
    public void putFileToHDFS() throws IOException, InterruptedException, URISyntaxException {

        // 1 获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://spark1:9000"), configuration, "luochan");

        // 2 创建输入流
        FileInputStream fis = new FileInputStream(new File("/home/luochan/dfsFliesDir/TIM"));

        // 3 获取输出流
        FSDataOutputStream fos = fs.create(new Path("/TIM/test.txt"));

        // 4 流对拷
        IOUtils.copyBytes(fis, fos, configuration);

        // 5 关闭资源
        IOUtils.closeStream(fos);
        IOUtils.closeStream(fis);
        fs.close();
    }


}
