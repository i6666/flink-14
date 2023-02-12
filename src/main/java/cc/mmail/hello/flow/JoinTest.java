package cc.mmail.hello.flow;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.util.concurrent.TimeUnit;

public class JoinTest {
    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 8081);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        DataStreamSource<String> stream1 = env.fromElements("1,aa,m,18", "2,bb,m,28", "3,cc,f,38");
        DataStreamSource<String> stream2 = env.fromElements("1:aa:m:18", "2:bb:m:28", "3:cc:f:38");
        DataStream<String> dataStream = coGroupTest(stream1, stream2);


        dataStream.print();
        /* * DataStream<T> result = one.coGroup(two)
         *     .where(new MyFirstKeySelector())
         *     .equalTo(new MyFirstKeySelector())
         *     .window(TumblingEventTimeWindows.of(Time.of(5, TimeUnit.SECONDS)))
         *     .apply(new MyCoGroupFunction());*/


        env.execute();

    }

    private static DataStream<String> coGroupTest(DataStreamSource<String> stream1, DataStreamSource<String> stream2) {
        DataStream<String> dataStream = stream1.coGroup(stream2).where(new KeySelector<String, String>() {
            @Override
            public String getKey(String value) throws Exception {
                return value;
            }
        }).equalTo(new KeySelector<String, String>() {
            @Override
            public String getKey(String value) throws Exception {
                return value;
            }
        }).window(TumblingEventTimeWindows.of(Time.of(5, TimeUnit.SECONDS))).apply(new CoGroupFunction<String, String, String>() {
            @Override
            public void coGroup(Iterable<String> first, Iterable<String> second, Collector<String> out) throws Exception {
                out.collect(first + ">>>" + second);
            }
        });
        return dataStream;
    }
}
