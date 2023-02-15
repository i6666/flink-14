package cc.mmail.hello.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class WordCountDriver {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

        // 创建一个job = mapper + reducer
        //Job job = new Job(new Configuration());//过期的方法
        Job job = Job.getInstance(new Configuration());
        //指定任务的入口
        job.setJarByClass(WordCountDriver.class);
        //指定任务的mapper，和输出的数据类型
        job.setMapperClass(WordCountMapper.class);

        job.setMapOutputKeyClass(Text.class); //就是k2的数据类型
        job.setMapOutputValueClass(IntWritable.class); //就是v2的数据类型

        //指定任务的reducer，和输出的数据类型
        job.setReducerClass(WordCountReducer.class);

        job.setOutputKeyClass(Text.class); //就是k4的数据类型
        job.setOutputValueClass(IntWritable.class);//就是v4的数据类型
        //指定输入和输出目录：HDFS的路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        //执行任务，true表示执行过程中打印日志，false表示不打印日志
        job.waitForCompletion(true);
    }
}
