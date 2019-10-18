package com.hadoop.test.mapreduce.filter;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author 王东升
 * @create 2019/10/15--16:16
 */
public class UserSearchCountMap extends Mapper<LongWritable,Text,Text,IntWritable> {
    Text userId = new Text();
    IntWritable valueOut = new IntWritable(1);
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String [] words = line.split("\t");
        userId.set(words[1]);
        context.write(userId,valueOut);
    }
}
