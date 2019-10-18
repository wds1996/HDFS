package com.hadoop.test.hdfs.sequencefile;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.BZip2Codec;

import java.io.IOException;
import java.net.URI;

/**
 * @author 王东升
 * @create 2019/10/14--16:17
 */
public class SequenceFileWrite {
    //模拟数据源；数组中一个元素表示一个文件的内容
    private static final String[] DATA = {
            "The Apache Hadoop software library is a framework that allows for the distributed processing of large data sets across clusters of computers using simple programming models.",
            "It is designed to scale up from single servers to thousands of machines, each offering local computation and storage.",
            "Rather than rely on hardware to deliver high-availability, the library itself is designed to detect and handle failures at the application layer",
            "o delivering a highly-available service on top of a cluster of computers, each of which may be prone to failures.",
            "Hadoop Common: The common utilities that support the other Hadoop modules."
    };
    public static void main(String[] args) {
        //输出路径
        String address = "hdfs://node01:8020/writeSequenceFile";
        Configuration config = new Configuration();
        //向HDFS上的此SequenceFile文件写数据
        Path path = new Path(address);
        //指定key和value的类型
        IntWritable key = new IntWritable();
        Text value = new Text();
        //sequencefile的一些配置
        SequenceFile.Writer.Option pathoption = SequenceFile.Writer.file(path);
        SequenceFile.Writer.Option keyoption = SequenceFile.Writer.keyClass(IntWritable.class);
        SequenceFile.Writer.Option valueoption = SequenceFile.Writer.keyClass(Text.class);
        try {
            FileSystem fs = FileSystem.get(URI.create(address), config);
        } catch (IOException e) {
            e.printStackTrace();
        }
        //SequenceFile 压缩方式  NONE | RECORD | BLOCK 三选一
        /*//1.NONE不指定压缩算法
        SequenceFile.Writer.Option compressOption = SequenceFile.Writer.compression(SequenceFile.CompressionType.NONE);
        try {
            SequenceFile.Writer writer = SequenceFile.createWriter(config,pathoption,keyoption,valueoption,compressOption);
        } catch (IOException e) {
            e.printStackTrace();
        }
        //2.RECORD不指定压缩算法
        SequenceFile.Writer.Option compressOption2 = SequenceFile.Writer.compression(SequenceFile.CompressionType.RECORD);
        try {
            SequenceFile.Writer writer = SequenceFile.createWriter(config,pathoption,keyoption,valueoption,compressOption);
        } catch (IOException e) {
            e.printStackTrace();
        }*/
        //方案三：使用BLOCK、压缩算法BZip2Codec；压缩耗时间
        //再加压缩算法
        BZip2Codec codec = new BZip2Codec();
        codec.setConf(config);
        SequenceFile.Writer.Option compressAlgorithm = SequenceFile.Writer.compression(SequenceFile.CompressionType.RECORD, codec);
        //创建写数据的Writer实例
        SequenceFile.Writer writer = null;
        try {
            writer = SequenceFile.createWriter(config, pathoption, keyoption, valueoption, compressAlgorithm);
            for (int i = 0; i < 1000; i++) {
                //分别设置key、value值
                key.set(1000 - i);
                value.set(DATA[i % DATA.length]); //%取模 3 % 3 = 0;
                System.out.printf("[%s]\t%s\t%s\n", writer.getLength(), key, value);
                //在SequenceFile末尾追加内容
                writer.append(key, value);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        //关闭流
        IOUtils.closeStream(writer);
    }
}
