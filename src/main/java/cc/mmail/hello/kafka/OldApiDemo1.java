package cc.mmail.hello.kafka;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;
import java.util.Properties;

public class OldApiDemo1 {
    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 8081);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);

        Properties properties = new Properties();
        //端口地址
        properties.setProperty("bootstrap.servers","localhost:9092");
        //读取偏移量
        properties.setProperty("auto.offset.reset","earliest");
        //设置消费组id
        properties.setProperty("group.id","g1");
        //没有开启checkpoint ，自动提交
        properties.setProperty("enable.auto.commit","true");
       // FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>("topic", new SimpleStringSchema(), properties);
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("172.19.39.52:9092")
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setGroupId("g1")
                .setTopics("topic01")
                .build();

        //1,a,100,1646784001000
        WatermarkStrategy<String> strategy = WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner(new SerializableTimestampAssigner<String>() {
                    @Override
                    public long extractTimestamp(String element, long recordTimestamp) {
                        String[] split = element.split(",");
                        return Long.parseLong(split[3]);
                    }
                });

        DataStreamSource<String> stream1 = env.fromSource(kafkaSource, strategy, "sourceName-test").setParallelism(2);
        stream1.print();

        env.execute();

    }
}
