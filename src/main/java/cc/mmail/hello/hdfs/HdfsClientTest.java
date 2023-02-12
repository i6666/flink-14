package cc.mmail.hello.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class HdfsClientTest {

    @Test
    public void testMkdirs() throws IOException, InterruptedException,
            URISyntaxException {
// 1 获取⽂文件系统
        Configuration configuration = new Configuration();
// 配置在集群上运⾏行行
// configuration.set("fs.defaultFS", "hdfs://linux121:9000"); // FileSystem fs = FileSystem.get(configuration);
        FileSystem fs = FileSystem.get(new URI("hdfs://linux121:9000"),
                configuration, "root");
        // 2 创建⽬目录
        fs.mkdirs(new Path("/hdfs-test"));
        // 3 关闭资源
        fs.close();
    }
}
