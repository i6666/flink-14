package cc.mmail.hello.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.annotation.PreDestroy;
import java.io.IOException;
import java.util.List;

public class HbaseClientDemo {

    private Connection connection;

    private Admin admin;

    @Before
    public void createCn() {

        Configuration that = HBaseConfiguration.create();
        that.set("hbase.zookeeper.quorum", "linux121,linux122");
        that.set("hbase.zookeeper.property.clientPort", "2181");

        try {
            System.out.println("开始创建hbase链接");
            connection = ConnectionFactory.createConnection(that);
            admin = connection.getAdmin();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @After
    public void destroy() {

        if (connection != null) {
            try {
                connection.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        if (admin != null) {
            try {
                admin.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    @Test
    public void putData() throws IOException {
        Table table = connection.getTable(TableName.valueOf("emp"));
        //row key
        Put put = new Put(Bytes.toBytes("110"));
        put.addColumn(Bytes.toBytes("base_info"),Bytes.toBytes("phone"),Bytes.toBytes("13344090909"));
        table.put(put);
        System.out.println("插⼊入成功!!");
        table.close();
    }

    @Test
    public void get() throws IOException {
        Table table = connection.getTable(TableName.valueOf("emp"));
        Get get = new Get(Bytes.toBytes("110"));
        Result result = table.get(get);
        Cell columnLatestCell = result.getColumnLatestCell(Bytes.toBytes("base_info"), Bytes.toBytes("phone"));
        System.out.println("getColumnLatestCell====="+columnLatestCell);
        Scan scan = new Scan();
        scan.setStartRow(Bytes.toBytes("11"));
        scan.setStopRow(Bytes.toBytes("111"));
        ResultScanner scanner = table.getScanner(scan);
        scanner.forEach(v-> System.out.println(v.getColumnLatestCell(Bytes.toBytes("base_info"), Bytes.toBytes("phone"))));
    }

    @Test
    public void createTable() throws IOException {
        HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf("emp"));
        HColumnDescriptor family = new HColumnDescriptor("base_info");
        hTableDescriptor.addFamily(family);
        admin.createTable(hTableDescriptor);
        System.out.println("表格创建成功");

    }


}
