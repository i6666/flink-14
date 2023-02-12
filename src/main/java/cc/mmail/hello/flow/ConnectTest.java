package cc.mmail.hello.flow;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.util.Collector;

public class ConnectTest {
    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 8081);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);

        //coMapTest(env);
        DataStreamSource<String> num = env.fromElements("1,2,3", "4,5,6");
        DataStreamSource<String> word = env.fromElements("a A","b B","c C","d D");
        //coFlatMapTest(num, word);

        DataStream<String> dataStream = num.union(word);

        dataStream.print();
        env.execute();
    }

    private static void coFlatMapTest(DataStreamSource<String> num, DataStreamSource<String> word) {
        SingleOutputStreamOperator<String> outputStreamOperator = num.connect(word).flatMap(new CoFlatMapFunction<String, String, String>() {
            @Override
            public void flatMap1(String value, Collector<String> out) throws Exception {
                for (String s : value.split(",")) {
                    out.collect(s);
                }

            }

            @Override
            public void flatMap2(String value, Collector<String> out) throws Exception {
                for (String s : value.split(" ")) {
                    out.collect(s);
                }
            }
        });
        outputStreamOperator.print();
    }

    private static void coMapTest(StreamExecutionEnvironment env) {
        DataStreamSource<Integer> num = env.fromElements(1, 2, 3, 4);
        DataStreamSource<String> word = env.fromElements("a","b","c","d");

        ConnectedStreams<String, Integer> connectedStreams = word.connect(num);
        SingleOutputStreamOperator<String> tuple2SingleOutputStream = connectedStreams.map(new CoMapFunction<String, Integer, String>() {

            @Override
            public String map1(String value) throws Exception {
                return value.toUpperCase();
            }

            @Override
            public String map2(Integer value) throws Exception {
                return String.valueOf(value * 100);
            }
        });
        tuple2SingleOutputStream.print();
    }
}
