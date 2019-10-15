package com.hadoop.test.mapreduce.dataclean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;
/**
 * @author 王东升
 * @create 2019/10/15--14:01
 */
public class DataClean {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //配置文件
        Configuration config = new Configuration();
        //实例化job
        Job job = Job.getInstance(config,DataClean.class.getSimpleName());
        //设置jar包
        job.setJarByClass(DataClean.class);
        //设置自定义map类
        job.setMapperClass(DataCleanMap.class);
        //设置map最后输出的key和value类型
        job.setMapOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        //设置输入输出路径
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        //由于没有reduce阶段，所以设置reduce tast为0
        job.setNumReduceTasks(0);
        //提交作业
        job.waitForCompletion(true);
    }
    public static class DataCleanMap extends Mapper<LongWritable,Text,Text,NullWritable> {
        //为了提高程序的效率，避免创建大量短周期的对象，出发频繁GC；此处生成一个对象，共用
        NullWritable nullValue = NullWritable.get();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //自定义计数器，用于记录残缺记录数
            Counter counter = context.getCounter("DataCleaning", "damagedRecord");
            //获取当前传入的value值，转换成字符串
            String line = value.toString();
            //split 分割字符
            String [] counts = line.toLowerCase().split("\t");
            //判断字段数组长度，是否为6
            if(counts.length != 6) {
                //若不是，则不输出，并递增自定义计数器
                counter.increment(1L);
            } else {
                //若是6，则原样输出
                context.write(value, nullValue);
            }
        }
    }
}
