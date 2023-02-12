package cc.mmail.hello.time;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.TimestampAssignerSupplier;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class TimeApiTest {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 8081);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        env.setParallelism(1);

        WatermarkStrategy<String> watermarkStrategy = WatermarkStrategy.<String>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<String>() {
            @Override
            public long extractTimestamp(String element, long recordTimestamp) {
                String[] split = element.split(",");
                return Long.parseLong(split[1]);
            }
        });

        SingleOutputStreamOperator<String> s1 = env.socketTextStream("localhost", 9098).assignTimestampsAndWatermarks(watermarkStrategy);
        SingleOutputStreamOperator<String> s2 = env.socketTextStream("localhost", 9099).assignTimestampsAndWatermarks(watermarkStrategy);

        DataStream<String> union = s1.union(s2);


        union.process(new ProcessFunction<String, String>() {
            @Override
            public void processElement(String value, ProcessFunction<String, String>.Context ctx, Collector<String> out) throws Exception {
                long currentWatermark = ctx.timerService().currentWatermark();
                Long timestamp = ctx.timestamp();
                System.out.println("timestamp:"+timestamp+",,, currentWatermark:"+currentWatermark+"===>"+value);
                out.collect(value);
            }
        }).print();

        env.execute();


    }
}
