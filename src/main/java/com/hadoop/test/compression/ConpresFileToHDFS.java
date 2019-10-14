package com.hadoop.test.compression;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.GzipCodec;
import java.io.*;
import java.net.URI;
/**
 * @author 王东升
 * @create 2019/10/13--18:06
 */
public class ConpresFileToHDFS {
    public static void main(String[] args) {
        //本地路径
        String local = "E:/大数据学习/scala课程/scala_day01.flv";
        //目标路径
        String aim_address = "hdfs://node01:8020/copyFromLocal/压缩测试.gz";
        //配置项
        Configuration config = new Configuration();
        //压缩类
        GzipCodec codec = new GzipCodec();
        codec.setConf(config);
        //BZip2Codec codec = new BZip2Codec();
        //输入输出流
        InputStream in;
        try {
            in = new BufferedInputStream(new FileInputStream(local));
            //文件系统
            FileSystem fileSystem = FileSystem.get(URI.create(aim_address),config);
            OutputStream out = fileSystem.create(new Path(aim_address));
            //压缩操作
            CompressionOutputStream compressionOutputStream = codec.createOutputStream(out);
            IOUtils.copyBytes(in,compressionOutputStream,4096,true);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
