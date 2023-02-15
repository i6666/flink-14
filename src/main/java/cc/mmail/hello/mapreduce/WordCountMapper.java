package cc.mmail.hello.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 *
 * @param <KEYIN> 输入key -v 文本偏移量，一行文本
 * @param <VALUEIN>
 * @param <KEYOUT> 输出 key -v
 * @param <VALUEOUT>
 */
public class WordCountMapper  extends Mapper<LongWritable, Text, Text, IntWritable> {

    Text text = new Text();
    IntWritable one = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        String str = value.toString();
        String[] words = str.split(" ");
        for (String word : words) {
            text.set(word);
            context.write(text,one);
        }
    }
}
