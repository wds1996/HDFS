package com.hadoop.test.mapreduce.selfpartitioner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

import java.util.HashMap;

/**
 * @author 王东升
 * @create 2019/10/17--16:10
 */
public class SelfPartitioner extends Partitioner<Text , IntWritable> {
    public static HashMap<String, Integer> dict = new HashMap<String, Integer>();

    //定义每个键对应的分区index，使用map数据结构完成
    static{
        dict.put("Dear", 0);
        dict.put("Bear", 1);
        dict.put("River", 2);
        dict.put("Car", 3);
    }
    public int getPartition(Text text, IntWritable intWritable, int i) {
        //根据业务逻辑，自定义分区规则
        int partitionIndex = dict.get(text.toString());
        return partitionIndex;
    }
    //自定义完分区规则后，在main方法中的job指定自定义分区的类，这里就采用之前的词频统计
}
