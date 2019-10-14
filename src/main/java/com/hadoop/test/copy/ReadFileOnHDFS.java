package com.hadoop.test.copy;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.net.URI;

/**
 * @author 王东升
 * @create 2019/10/10--14:58
 */
public class ReadFileOnHDFS {
    public static void main(String[] args) {
        //hadoop文件路径
        //String hdfsAddrs = args[0];
        String hdfsAddrs = "hdfs://node01:8020/test.txt";
        //本地文件路径
        //String hdfsaddrs = args[1];
        String hdfsaddrs = "D:/FDFStest.txt";
        //创建配置文件，创建文件系统
        Configuration config = new Configuration();
        try {
            FileSystem fileSystem = FileSystem.get(URI.create(hdfsAddrs),config);
            FSDataInputStream FSDinput = fileSystem.open(new Path(hdfsAddrs));
            BufferedOutputStream outputStream = new BufferedOutputStream(new FileOutputStream(hdfsaddrs));
            IOUtils.copyBytes(FSDinput,outputStream,4096,true);
        } catch (Exception e) {
            System.out.println("发生异常了");
            e.printStackTrace();
        }

    }
}
