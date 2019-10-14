package com.hadoop.test.copy;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

/**
 * @author 王东升
 * @create 2019/10/10--14:58
 */
public class FileToHDFS {
    public static void main(String[] args) {
        //本地文件路径
        String localsource = "D:/test.txt";
        //String localsource = args[0];
        //HDFS保存的文件路径
        String hdfsaddrs = "hdfs://node01:8020/test.txt";
        //String hdfsaddrs = args[1];
        try {
            //开启一个输入流，并用缓冲包装一下
            InputStream in = new BufferedInputStream(new FileInputStream(localsource));
            //HDFS操作，HDFS读写的配置文
            Configuration configuration = new Configuration();
            //创建HDFS文件系统
            FileSystem fileSystem = FileSystem.get(URI.create(hdfsaddrs),configuration);
            //调用Filesystem的create方法返回的是FSDataOutputStream对象
            //该对象不允许在文件中定位，因为HDFS只允许一个已打开的文件顺序写入或追加
            OutputStream out = fileSystem.create(new Path(hdfsaddrs));
            IOUtils.copyBytes(in,out,4096,true);
        }catch (Exception e){
            System.out.println("发生异常了");
            System.out.println(e.getClass());
            e.printStackTrace();
        }
    }
}
