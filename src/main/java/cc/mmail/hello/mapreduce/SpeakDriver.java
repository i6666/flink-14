package cc.mmail.hello.mapreduce;

import cc.mmail.hello.mapreduce.bean.SpeakOutBean;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SpeakDriver {

    public static void main(String[] args) throws Exception {

        Job job = Job.getInstance();
        job.setJarByClass(SpeakDriver.class);

        // Specify various job-specific parameters
        job.setJobName("SpeakDriverJob");

        job.setMapperClass(SpeakDurationMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(SpeakOutBean.class);

        job.setReducerClass(SpeakDurationReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(SpeakOutBean.class);

        //指定输入和输出目录：HDFS的路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // Submit the job, then poll for progress until the job is complete
        job.waitForCompletion(true);
    }
}
