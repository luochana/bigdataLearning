package luochan.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class ETLDriver implements Tool {

    private Configuration conf;

    public int run(String[] strings) throws Exception {

        //1.获取job对象.
        Job job = Job.getInstance(conf);

        //2.设置jar包路径
        job.setJarByClass(ETLDriver.class);

        //3.设置Mapper类和输出kv类型
        job.setMapperClass(ETLMapper.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);

        //4.设置输出KV类型
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        //5.设置输入输出路径
        FileInputFormat.setInputPaths(job,new Path(strings[0]));
        FileOutputFormat.setOutputPath(job,new Path(strings[1]));

        //6.提交任务
        boolean result = job.waitForCompletion(true);

        return result?0:1;
    }

    public void setConf(Configuration configuration) {
        conf=configuration;
    }

    public Configuration getConf() {
        return conf;
    }

    public static void main(String[] args) {

        try {
            int run = ToolRunner.run(new Configuration(), new ETLDriver(), args);
            System.out.println(run);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
