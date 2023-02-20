package cc.mmail.hello.mapreduce.comment;

import cc.mmail.hello.mapreduce.SpeakDriver;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;

public class MergeDriver {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

        Job job = Job.getInstance();
        job.setJarByClass(SpeakDriver.class);

        // Specify various job-specific parameters
        job.setJobName("MergeDriver");

        job.setMapperClass(MergeMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(BytesWritable.class);

        job.setReducerClass(MergeReduce.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(BytesWritable.class);

        job.setInputFormatClass(MergeInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        //指定输入和输出目录：HDFS的路径
        FileInputFormat.setInputPaths(job, new Path("/Users/strong/Downloads/bigdata/workTxt/input"));
        FileOutputFormat.setOutputPath(job, new Path("/Users/strong/Downloads/bigdata/workTxt/output"));

        // Submit the job, then poll for progress until the job is complete
        job.waitForCompletion(true);
    }
}
