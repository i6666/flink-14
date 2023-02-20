package cc.mmail.hello.mapreduce.comment;

import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MergeReduce extends Reducer<Text, ByteWritable,Text, ByteWritable> {

    @Override
    protected void reduce(Text key, Iterable<ByteWritable> values, Reducer<Text, ByteWritable, Text, ByteWritable>.Context context) throws IOException, InterruptedException {

        context.write(key,values.iterator().next());
    }
}
