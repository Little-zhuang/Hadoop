package com.zhuang.hdfs_api;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;


/**
 * @author Elke
 * @Description 通过文件系统来操作文件
 * @date 2021/2/23-16:17
 */
public class hdfsDemo {

    /*
    * 获取FileSystem:方式1
    * FileSystem.get(Configuration conf)
    * */

    @Test
    public void getFileSystem1() throws IOException {
        //1:创建Configuration对象
        Configuration configuration = new Configuration();

        //2:设置文件系统的类型（本地还是分布式）
        configuration.set("fs.defaultFS","hdfs://192.168.91.100:9000");

        //3:获取指定的文件系统
        FileSystem fileSystem = FileSystem.get(configuration);

        //4:输出FileSystem内容
        System.out.println(fileSystem);
    }

    /*
    * 获取FileSystem:方式2
    * FileSystem.get(URI uri, Configuration conf)
    * */

    @Test
    public void getFileSystem2() throws Exception{
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://192.168.91.100:9000"), new Configuration());
        System.out.println("fileSystem"+ fileSystem);
    }

    /*
    * 获取FileSystem:方式3
    * FileSystem.newInstance(Configuration conf)
    * */

    @Test
    public void getFileSystem3() throws IOException {
        Configuration configuration = new Configuration();

        //指定文件类型
        configuration.set("fs.defaultFS","hdfs://192.168.91.100:9000");

        FileSystem fileSystem = FileSystem.newInstance(configuration);

        System.out.println(fileSystem);
    }

    /*
     * 获取FileSystem:方式4
     * FileSystem.newInstance(URI uri, Configuration conf)
     * */

    @Test
    public void getFileSystem4() throws URISyntaxException, IOException {
        FileSystem fileSystem = FileSystem.newInstance(new URI("hdfs://192.168.91.100:9000"), new Configuration());

        System.out.println(fileSystem);
    }

    /*
    * hdfs文件的遍历
    * */

    @Test
    public void listMyFile() throws URISyntaxException, IOException {
        //1. 获取fileSystem实例
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://192.168.91.100:9000"), new Configuration());

        //2，调用listFiles方法，获取/ 目录下所有的文件信息
        RemoteIterator<LocatedFileStatus> iterator = fileSystem.listFiles(new Path("/"), true);

        //3. 通过迭代器遍历出来文件
        while (iterator.hasNext()){
            LocatedFileStatus fileStatus = iterator.next();

            //获取文件的绝对路径 hdfs://192.168.91.100:9000/xxx
            Path path = fileStatus.getPath();
            System.out.println("文件路径是：" + path + "  文件名：" + path.getName());

            //获取分区大小
            long blockSize = fileStatus.getBlockSize();
            System.out.println("分区大小是：" + blockSize);

            //获取分区数量
            int length = fileStatus.getBlockLocations().length;
            System.out.println("分区数量是：" + length);

            //获取备份数量
            short replication = fileStatus.getReplication();
            System.out.println("备份数量是：" + replication);
        }
    }
}
