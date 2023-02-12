package cc.mmail.hello.process;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class ProcessFunction_1 {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 8081);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        env.setParallelism(1);

        DataStreamSource<String> stream1 = env.socketTextStream("localhost", 9098);

        DataStream<Tuple2<String, Integer>> s1 = stream1.process(new ProcessFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void processElement(String value, ProcessFunction<String, Tuple2<String, Integer>>.Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
                if (value.length() > 5) {
                    ctx.output(new OutputTag<>("length gt 5", TypeInformation.of(String.class)), value);
                }
                String[] arr = value.split(",");

                out.collect(Tuple2.of(arr[0], Integer.valueOf(arr[1])));
            }

            @Override
            public void open(Configuration parameters) throws Exception {
                RuntimeContext context = getRuntimeContext();
                String taskName = context.getTaskName();
                System.out.println("taskName:" + taskName);
                super.open(parameters);
            }

            @Override
            public void close() throws Exception {
                super.close();
            }
        });

        KeyedStream<Tuple2<String, Integer>, String> keyedStream = s1.keyBy((KeySelector<Tuple2<String, Integer>, String>) value -> value.f0);
        DataStream<Tuple2<String, Integer>> process = keyedStream.process(new KeyedProcessFunction<String, Tuple2<String, Integer>, Tuple2<String, Integer>>() {
            @Override
            public void processElement(Tuple2<String, Integer> value, KeyedProcessFunction<String, Tuple2<String, Integer>, Tuple2<String, Integer>>.Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
                out.collect(Tuple2.of(value.f0 + "...", value.f1 * 1000));
            }
        });

        process.print("keyedStream");
        env.execute();
    }
}
