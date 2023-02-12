package cc.mmail.hello.windows;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.concurrent.TimeUnit;

/**
 * 事件，处理，滑动，滚动，计数
 * 增量聚合算子 min max sum minBy maxBy  reduce aggregate
 * 全量聚合算子 apply process
 */
public class WindowsTest {
    public static void main(String[] args) {

        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 8081);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);

        DataStreamSource<String> source = env.socketTextStream("localhost", 9098);
        source.windowAll(TumblingProcessingTimeWindows.of(Time.seconds(5))).reduce(new ReduceFunction<String>() {
            //滚动聚合算子，它有个限制 ，聚合结果的数据类型 与 数据源中的数据类型 ，是一致
            @Override
            public String reduce(String value1, String value2) throws Exception {
                return null;
            }
        }); //处理时间语义，滚动窗口
        source.windowAll(SlidingProcessingTimeWindows.of(Time.of(1, TimeUnit.HOURS), Time.of(10, TimeUnit.MINUTES))).aggregate(new AggregateFunction<String, Object, Object>() {
            @Override
            public Object createAccumulator() {
                return null;
            }

            @Override
            public Object add(String value, Object accumulator) {
                return null;
            }

            @Override
            public Object getResult(Object accumulator) {
                return null;
            }

            @Override
            public Object merge(Object a, Object b) {
                return null;
            }
        });//处理时间语义，滑动窗口

        source.countWindowAll(10,2).min(1);
        source.countWindowAll(10);
    }
}
