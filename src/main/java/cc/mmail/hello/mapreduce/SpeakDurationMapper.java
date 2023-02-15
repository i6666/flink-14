package cc.mmail.hello.mapreduce;

import cc.mmail.hello.mapreduce.bean.SpeakOutBean;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SpeakDurationMapper extends Mapper<LongWritable, Text,Text, SpeakOutBean> {

    Text text = new Text();

    SpeakOutBean outBean =  new SpeakOutBean();

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, SpeakOutBean>.Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] split = line.split(" ");

        outBean.setOtherDuration(Long.valueOf(split[4]));
        outBean.setSelfDuration(Long.valueOf(split[5]));
        outBean.setSumDuration(outBean.getOtherDuration(),outBean.getSelfDuration());
        text.set(split[1]);
        context.write(text,outBean);


    }
}
