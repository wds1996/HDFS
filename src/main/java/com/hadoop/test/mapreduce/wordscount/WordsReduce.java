package com.hadoop.test.mapreduce.wordscount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author 王东升
 * @create 2019/10/15--10:30
 */
public class WordsReduce extends Reducer<Text,IntWritable,Text,IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        //单词计数
        int sum = 0;
        //将单词树进行累加
        for (IntWritable conunt:values) {
            sum += conunt.get();
        }
        //重新组装键值对，进行输出
        context.write(key,new IntWritable(sum));
    }
}
