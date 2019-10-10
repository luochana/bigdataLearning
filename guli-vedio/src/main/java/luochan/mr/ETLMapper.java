package luochan.mr;


import luochan.util.ETLUtil;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class ETLMapper extends Mapper<LongWritable, Text, NullWritable,Text> {

    private Text v=new Text();


    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        //1.获取数据
        String oriStr = value.toString();

        //2.清晰数据
        String etlStr = ETLUtil.etlStr(oriStr);

        //3.写出
        if(etlStr==null)
            return;

        v.set(etlStr);
        context.write(NullWritable.get(),v);
    }
}
