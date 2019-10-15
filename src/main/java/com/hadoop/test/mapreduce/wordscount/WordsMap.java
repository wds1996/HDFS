package com.hadoop.test.mapreduce.wordscount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author 王东升
 * @create 2019/10/15--10:20
 */
public class WordsMap extends Mapper<LongWritable,Text,Text,IntWritable> {

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //获取当前传入的value值，转换成字符串
        String line = value.toString();
        //split 分割字符
        String [] words = line.toLowerCase().replaceAll("\\pP|\\pS", "").split(" ");

        //将所有单词以键值对的方式传递出去
        for (String word : words) {
            context.write(new Text(word),new IntWritable(1));
        }
    }
}
