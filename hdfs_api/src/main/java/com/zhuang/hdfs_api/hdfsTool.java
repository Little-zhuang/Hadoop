package com.zhuang.hdfs_api;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.QuotaUsage;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @author Elke
 * @Description hdfs工具
 * @date 2021/2/25-16:34
 */
public class hdfsTool {

    /*
     * hdfs上创建文件夹
     * mkdirs可以递归创建
     * */

    @Test
    public void mkDir() throws URISyntaxException, IOException {

        FileSystem fileSystem = FileSystem.get(new URI("hdfs://192.168.91.100:9000"), new Configuration());

        boolean mkdirs = fileSystem.mkdirs(new Path("/hello/dirs/test"));

        fileSystem.close();
    }

    /*
     * hdfs上删除文件夹
     * */

    @Test
    public void deleteDir() throws URISyntaxException, IOException {
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://192.168.91.100:9000"), new Configuration());

        //true表示删除当前目录及目录下的内容，false表示只删除当前目录，如果目录里面有内容就删除失败
        fileSystem.delete(new Path("/hello"), true);
        fileSystem.close();
    }

    /*
     * hdfs上修改文件夹名字
     * */

    @Test
    public void rnameDir() throws URISyntaxException, IOException {
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://192.168.91.100:9000"), new Configuration());

        fileSystem.rename(new Path("/hello"), new Path("/hello2"));

        fileSystem.close();
    }

    /*
     * hdfs上下载文件(hdfs文件拷贝下来)
     * 方法1
     * */

    @Test
    public void getFileToLocal1() throws IOException, URISyntaxException {
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://192.168.91.100:9000"), new Configuration());

        //要下载的文件路径及内容
        FSDataInputStream inputStream = fileSystem.open(new Path("/usr/a.txt"));

        //要下载到那个位置
        FileOutputStream outputStream = new FileOutputStream(new File("E:\\base_java\\Hadoop_project\\hdfs_api\\download\\a2.txt"));

        IOUtils.copy(inputStream, outputStream);
        IOUtils.closeQuietly(inputStream);
        IOUtils.closeQuietly(outputStream);
        fileSystem.close();
    }

    /*
     * hdfs上传文件(本地文件拷贝到hdfs上)
     * */

    @Test
    public void putFrameLocal() throws IOException, URISyntaxException {
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://192.168.91.100:9000"), new Configuration());

        fileSystem.copyFromLocalFile(new Path("E:\\base_java\\Hadoop_project\\hdfs_api\\download\\down.txt"), new Path("/hello2/dirs/test/down.txt"));

        fileSystem.close();
    }

    /*
     * hdfs上下载文件
     * (可伪装成root用户来操作文件)
     * 方法2
     * */

    @Test
    public void getFileToLocal2() throws IOException, URISyntaxException, InterruptedException {
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://192.168.91.100:9000"), new Configuration(),"root");

        fileSystem.copyToLocalFile(new Path("/hello2/dirs/test/down.txt"), new Path("E:\\base_java\\Hadoop_project\\hdfs_api\\download\\down.txt"));

        fileSystem.close();
    }

    /*
     * 获取文件数量限额
     * */

    @Test
    public void test() throws IOException, URISyntaxException {
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://192.168.91.100:9000"), new Configuration());
        QuotaUsage usage = fileSystem.getQuotaUsage(new Path("/test/dir1"));
        System.out.println(usage.getQuota());
    }
}
