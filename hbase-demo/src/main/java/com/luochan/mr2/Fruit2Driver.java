package com.luochan.mr2;

import com.luochan.mr1.FruitDriver;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Fruit2Driver implements Tool {

    private Configuration conf=null;
    public int run(String[] strings) throws Exception {

        //1.
        Job job = Job.getInstance(conf);

        //2.
        job.setJarByClass(Fruit2Driver.class);

        //3.
        TableMapReduceUtil.initTableMapperJob(strings[0],new Scan(),Fruit2Mapper.class, ImmutableBytesWritable.class, Put.class,job);

        //4.
        TableMapReduceUtil.initTableReducerJob(strings[1],Fruit2Reducer.class,job);

        //5.
        boolean b = job.waitForCompletion(true);

        return b?0:1;
    }

    public void setConf(Configuration configuration) {
        conf=configuration;
    }

    public Configuration getConf() {
        return conf;
    }

    public static void main(String[] args) {

      //  Configuration configuration = HBaseConfiguration.create();
        try {
            Configuration configuration = new Configuration();
            ToolRunner.run(configuration,new FruitDriver(),args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
