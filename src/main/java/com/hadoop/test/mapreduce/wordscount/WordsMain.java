package com.hadoop.test.mapreduce.wordscount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * @author 王东升
 * @create 2019/10/15--10:38
 */
public class WordsMain{
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //判断参数个数
        if (args.length != 2 || args == null){
            System.out.println("请输入参数路径");
            System.exit(0);
        }
        //创建配置项
        Configuration config = new Configuration();
        //生成job实例
        Job job = Job.getInstance(config,WordsMain.class.getSimpleName());
        //设置job的jar包，一般设置为主类
        job.setJarByClass(WordsMain.class);
        //设置MR输入输出格式。默认是TextInputFormat.class,TextOutputFormat.class;
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        //设置输入输出路径
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        //设置自定义map类
        job.setMapperClass(WordsMap.class);
        //设置map combine类，减少网络传输量
        job.setCombinerClass(WordsReduce.class);
        //设置reduce类
        job.setReducerClass(WordsReduce.class);

        //设置map输出的key和value类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        //设置reduce 最终输出的key和value类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //提交作业
        job.waitForCompletion(true);
    }
}
