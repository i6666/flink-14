package cc.mmail.hello.mapreduce;

import cc.mmail.hello.mapreduce.bean.UserBean;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class ReduceJoinMapper extends Mapper<LongWritable, Text,LongWritable, UserBean> {
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, LongWritable, UserBean>.Context context) throws IOException, InterruptedException {
        super.map(key, value, context);
    }

    @Override
    protected void setup(Mapper<LongWritable, Text, LongWritable, UserBean>.Context context) throws IOException, InterruptedException {
    }
}

