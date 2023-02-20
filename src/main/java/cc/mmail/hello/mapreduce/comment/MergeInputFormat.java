package cc.mmail.hello.mapreduce.comment;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

/**
 * 小文件合并，key未路径，value为bytes
 */
public class MergeInputFormat extends CombineFileInputFormat<Text, BytesWritable> {

    @Override
    protected boolean isSplitable(JobContext context, Path filename) {
        return false;
    }

    @Override
    public RecordReader<Text, BytesWritable> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException {
        MergeRecordReader customRecordReader = new MergeRecordReader();
        try {
            customRecordReader.initialize(split,context);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return customRecordReader;
    }
}
