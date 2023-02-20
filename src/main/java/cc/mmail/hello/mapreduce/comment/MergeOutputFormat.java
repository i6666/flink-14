package cc.mmail.hello.mapreduce.comment;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import javax.xml.soap.Text;
import java.io.IOException;

public class MergeOutputFormat extends FileOutputFormat<Text, NullWritable> {
    @Override
    public RecordWriter<Text, NullWritable> getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException {

        Configuration configuration = job.getConfiguration();
        FileSystem fileSystem = FileSystem.get(configuration);
        fileSystem.create(new Path(""));
        fileSystem.create(new Path(""));

        return null;
    }
}
