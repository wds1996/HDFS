package com.hadoop.test.mapreduce.compression;

import com.hadoop.test.mapreduce.filter.UserSearchCountMap;
import com.hadoop.test.mapreduce.filter.UserSearchCountReduce;
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
 * @create 2019/10/17--16:40
 */
public class Compress {
    //这里以统计用户次数的例子，开启压缩
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //配置项
        Configuration configuration = new Configuration();
        //设置压缩
        //开启map输出进行压缩的功能
        configuration.set("mapreduce.map.output.compress", "true");
        //设置map输出的压缩算法是：BZip2Codec，它是hadoop默认支持的压缩算法，且支持切分
        configuration.set("mapreduce.map.output.compress.codec", "org.apache.hadoop.io.compress.BZip2Codec");
        //开启job输出压缩功能
        configuration.set("mapreduce.output.fileoutputformat.compress", "true");
        //指定job输出使用的压缩算法
        configuration.set("mapreduce.output.fileoutputformat.compress.codec", "org.apache.hadoop.io.compress.BZip2Codec");
        //实例化job
        Job job = Job.getInstance(configuration,Compress.class.getSimpleName());
        //指定jar包
        job.setJarByClass(Compress.class);
        //指定文件输入输出路径
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        //指定MapReduce过程输出格式
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        //自定义map类
        job.setMapperClass(UserSearchCountMap.class);
        //map的key——value格式
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        ///自定义reduce类
        job.setReducerClass(UserSearchCountReduce.class);
        //reducekey——value格式
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        //提交job
        job.waitForCompletion(true);
    }
}
