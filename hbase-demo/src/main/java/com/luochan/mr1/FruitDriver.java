package com.luochan.mr1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class FruitDriver implements Tool {

    private Configuration conf=null;

    public int run(String[] strings) throws Exception {
        //1.
        Job job = Job.getInstance(conf);

        //2.
        job.setJarByClass(FruitDriver.class);

        //3.
        job.setMapperClass(FruitMapper.class);

        //4.
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);

        //5.
        TableMapReduceUtil.initTableReducerJob(strings[1],FruitReducer.class,job);

        //6.
        FileInputFormat.setInputPaths(job,new Path(strings[0]));

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
        try {
            ToolRunner.run(new Configuration(),new FruitDriver(),args);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
