package cc.mmail.hello.flow;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class SplitTest {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 8081);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);

        DataStreamSource<Integer> elements = env.fromElements(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

        OutputTag<Integer> ouShu = new OutputTag<Integer>("ouShu"){};
        OutputTag<Integer> jiShu = new OutputTag<Integer>("jiShu"){};
        SingleOutputStreamOperator<Integer> outputStreamOperator = elements.process(new ProcessFunction<Integer, Integer>() {
            @Override
            public void processElement(Integer value, ProcessFunction<Integer, Integer>.Context ctx, Collector<Integer> out) throws Exception {
                if (value % 2 == 0) {
                    //输出到侧面流
                    ctx.output(ouShu, value);
                } else {
                    ctx.output(jiShu, value);
                }

                //主流
                out.collect(value);
            }
        });

        DataStream<Integer> ouShuStream = outputStreamOperator.getSideOutput(ouShu);
        ouShuStream.print("ouShuStream");
        DataStream<Integer> jiShuStream = outputStreamOperator.getSideOutput(jiShu);
        jiShuStream.print("jiShuStream");
        outputStreamOperator.print("outputStreamOperator");
        env.execute();
    }
}
