package cc.mmail.hello.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;

public class HdfsClientTest {

    @Test
    public void testMkdirs() throws IOException {
        FileSystem fs = getFileSystem();
        // 2 创建⽬目录
        fs.mkdirs(new Path("/hdfs-test"));
        // 3 关闭资源
        fs.close();
    }


    @Test
    public void testCopyFileFromLocal() throws IOException {
        FileSystem fs = getFileSystem();
        fs.copyFromLocalFile(new Path("/Users/strong/IdeaProjects/dc-guide/flink-14/src/main/resources/people.json"), new Path("/hdfs-test/people.json"));
        fs.close();
    }

    @Test
    public void optFileSystem() throws IOException {
        FileSystem fs = getFileSystem();
        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);

        while (listFiles.hasNext()) {
            LocatedFileStatus fileStatus = listFiles.next();
            System.out.println("getPermission" + fileStatus.getPermission());
            System.out.println("getReplication" + fileStatus.getReplication());
            System.out.println("getPath" + fileStatus.getPath());
            System.out.println("getGroup" + fileStatus.getGroup());
            BlockLocation[] blockLocations = fileStatus.getBlockLocations();
            for (BlockLocation blockLocation : blockLocations) {
                // 获取块存储的主机节点
                String[] hosts = blockLocation.getHosts();
                System.out.println(Arrays.toString(hosts));
            }
        }

        fs.close();

    }
    @Test
    public void testListStatus() throws IOException {
        FileSystem fs = getFileSystem();
        FileStatus[] fileStatuses = fs.listStatus(new Path("/"));
        for (FileStatus fileStatus : fileStatuses) {
            if (fileStatus.isFile()){
                System.out.println(fileStatus.getPath().getName()+"是文件");
            }else {
                System.out.println(fileStatus.getPath().getName()+"是文件夹");
            }
        }
        fs.close();
    }

    @Test
    public void upFile() throws Exception {
        FileSystem fs = getFileSystem();

        FileInputStream inputStream = new FileInputStream("/Users/strong/IdeaProjects/dc-guide/flink-14/src/main/resources/log4j.properties");

        FSDataOutputStream outputStream = fs.create(new Path("/hdfs-test/log4j.properties"));
        IOUtils.copyBytes(inputStream,outputStream,2048);

    }

    @Test
    public void downFile() throws IOException {
        FileSystem fs = getFileSystem();
        FSDataInputStream open = fs.open(new Path("/hdfs-test/log4j.properties"));

        FileOutputStream fileOutputStream = new FileOutputStream("/Users/strong/IdeaProjects/dc-guide/flink-14/src/main/resources/sss.properties");

        IOUtils.copyBytes(open,fileOutputStream,2048);


    }



    private static FileSystem getFileSystem() {
        // 1 获取⽂文件系统
        Configuration configuration = new Configuration();
// 配置在集群上运⾏行行
// configuration.set("fs.defaultFS", "hdfs://linux121:9000"); // FileSystem fs = FileSystem.get(configuration);
        FileSystem fs = null;
        try {
            fs = FileSystem.get(new URI("hdfs://linux121:9000"),
                    configuration, "root");
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
        return fs;
    }

}
