package cc.mmail.hello;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

public class WindowWordCount {
    public static void main(String[] args) throws Exception {


//        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
//
//        DataStream<Tuple2<String, Integer>> ds = environment.socketTextStream("localhost", 9999)
//                .flatMap((FlatMapFunction<String, Tuple2<String, Integer>>) (value, out) -> {
//                    //通过TypeHint 捕获泛型信息
//                }, TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {
//                }))
//                .keyBy(value -> value.f0).window(TumblingProcessingTimeWindows.of(Time.seconds(5))).sum(1);
//        ds.print();
//
//        environment.execute();


        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 8081);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);

        SingleOutputStreamOperator<Tuple2<String, Integer>> dataStream = env.socketTextStream("localhost", 9999)
                .flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(String s, Collector<Tuple2<String, Integer>> out) throws Exception {
                        for (String word : s.split(" ")) {
                            out.collect(new Tuple2<>(word, 1));
                        }
                    }
                    //滚动窗口 聚合
                }).keyBy(value -> value.f0).window(TumblingProcessingTimeWindows.of(Time.seconds(5))).sum(1);

        dataStream.print();

        env.execute("Window WordCount");

    }
}
