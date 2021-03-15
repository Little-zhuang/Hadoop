package com.zhuang.hdfs_api;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

/**
 * @author Elke
 * @Description 通过url的方式访问文件(简单掌握)
 * @date 2021/2/23-15:44
 */
public class uriDemo {
    @Test
    public void urlDemo() throws IOException {
//        1.注册一个url
        URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
//        2.获取hdfs文件的输入流
        InputStream inputStream = new URL("hdfs://192.168.91.100:9000/test/dir2/a.txt").openStream();
//        3.获取本地文件的输出流(下载到哪个位置)
        FileOutputStream outputStream = new FileOutputStream(new File("E:\\base_java\\Hadoop_project\\hdfs_api\\download\\hello.txt"));
//        4.实现文件的拷贝
        IOUtils.copy(inputStream,outputStream);
//        5.关闭文件的输入输出流
        IOUtils.closeQuietly(inputStream);
        IOUtils.closeQuietly(outputStream);
    }
}
