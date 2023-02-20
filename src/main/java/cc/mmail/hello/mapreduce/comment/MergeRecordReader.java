package cc.mmail.hello.mapreduce.comment;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;

public class MergeRecordReader extends RecordReader<Text, BytesWritable> {

    CombineFileSplit fileSplit;
    Configuration config;
    Text key = new Text();

    BytesWritable value = new BytesWritable();

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        fileSplit = (CombineFileSplit) split;
        config = context.getConfiguration();
    }

    boolean flag = true;


    Iterator<Path> iterator = null;
    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (iterator == null){
            iterator = Arrays.stream(fileSplit.getPaths()).iterator();
        }
        if (iterator.hasNext()) {
            Path path = iterator.next();
            FileSystem fileSystem = path.getFileSystem(config);
            long splitLength = fileSystem.getFileStatus(path).getLen();
            byte[] content = new byte[(int) splitLength];

            //读取数据
            FSDataInputStream fsDataInputStream = fileSystem.open(path);


            IOUtils.readFully(fsDataInputStream, content, 0, (int) splitLength);


            value.set(content, 0, content.length);
            key.set(path.toString());

            IOUtils.closeStream(fsDataInputStream);
            return true;
        }
        return false;
    }


    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    @Override
    public BytesWritable getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return 0;
    }

    @Override
    public void close() throws IOException {

    }
}
