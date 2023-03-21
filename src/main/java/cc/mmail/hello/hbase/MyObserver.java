package cc.mmail.hello.hbase;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;

import java.io.IOException;

public class MyObserver extends BaseRegionObserver {

    @Override
    public void prePut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, Durability durability) throws IOException {

        /**
         * t2 表也插入一条
         */
        Table table = e.getEnvironment().getTable(TableName.valueOf("t2"));
        Cell cell = put.get("info".getBytes(), "name".getBytes()).get(0);
        Put put1 = new Put(put.getRow());
        put1.add(cell);
        table.put(put);
    }
}
