package cc.mmail.hello;

import org.apache.flink.api.connector.source.lib.NumberSequenceSource;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.NumberSequenceIterator;
import org.apache.flink.util.SplittableIterator;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class FromElementDemo {

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 8081);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        env.setParallelism(3);
//        DataStreamSource<String> source = getStringDataStreamSource(env);
        DataStreamSource<Long> source = env.generateSequence(1, 20);
        source.print();
        env.execute("");


    }

    /**
     *
     //1,fromParallelCollection
     //2,fromElements
     //3,fromCollection
     * @param env
     * @return
     */
    private static DataStreamSource<String> getStringDataStreamSource(StreamExecutionEnvironment env) {
        List<String> list = new ArrayList<>();
        list.add("flink");
        list.add("spark");
        list.add("flink");
//        DataStreamSource<String> fromCollection = env.fromCollection(list);

        //1,fromParallelCollection
        //2,fromElements
        //3,fromCollection
        DataStreamSource<Long> numbers = env.fromParallelCollection(new NumberSequenceIterator(1, 200), Long.class);

        DataStreamSource<String> source = env.fromElements("flink", "spark", "flink");
        return source;
    }
}
