package cc.mmail.hello.mapreduce.comment;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;

public class MergeRecordWriter implements RecordWriter<Text, BytesWritable> {

    @Override
    public void write(Text key, BytesWritable value) throws IOException {

    }

    @Override
    public void close(Reporter reporter) throws IOException {

    }
}
