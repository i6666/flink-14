package cc.mmail.hello.time;

import cc.mmail.hello.windows.EventBean;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * 需求一: 每隔5s，统计最近20s的数据中，每个用户的行为事件条数
 * 需求二: 每隔10s，统计最近30s的数据中，每个用户的平均每次行为时长
 * 需求三: 每隔10s，统计最近30s的数据中，每个用户的行为事件中，行为时长最长的前2条记录
 */
public class EventBeanTest {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 8081);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);

        // 1,20,1000
        // 1,10,1000
        // 2,20,2000
        // 1,10,2000
        // 2,20,3000
        DataStreamSource<String> source = env.socketTextStream("localhost", 9098);


        WatermarkStrategy<EventBean> watermarkStrategy = WatermarkStrategy.<EventBean>forMonotonousTimestamps().withTimestampAssigner((SerializableTimestampAssigner<EventBean>) (element, recordTimestamp) -> element.getEventTime());


        SingleOutputStreamOperator<EventBean> watermarkStreams = source.map(value -> {
            String[] split = value.split(",");
            return new EventBean(Long.parseLong(split[0]), Long.parseLong(split[1]), Long.parseLong(split[2]));
        }).assignTimestampsAndWatermarks(watermarkStrategy);


        SingleOutputStreamOperator<Integer> aggregate = test1(watermarkStreams);
        SingleOutputStreamOperator<Double> aggregate2 = test2(watermarkStreams);
        SingleOutputStreamOperator<Integer> aggregate3 = test3(watermarkStreams);


        aggregate.print();

        env.execute();

    }

    private static SingleOutputStreamOperator<Integer> test3(SingleOutputStreamOperator<EventBean> watermarkStreams) {
        watermarkStreams.keyBy(EventBean::getWid).window(SlidingEventTimeWindows.of(Time.seconds(20),Time.seconds(10)))
                //输入，输出，key，窗口
                .apply(new WindowFunction<EventBean, EventBean, Long, TimeWindow>() {
                    @Override
                    public void apply(Long wid, TimeWindow window, Iterable<EventBean> input, Collector<EventBean> out) throws Exception {
                        Stream<EventBean> stream = StreamSupport.stream(input.spliterator(), false);
                        List<EventBean> collect = stream.sorted(new Comparator<EventBean>() {
                            @Override
                            public int compare(EventBean o1, EventBean o2) {
                                return (int) (o2.getActTimeLong() - o1.getActTimeLong());
                            }
                        }).limit(2).collect(Collectors.toList());

                        collect.forEach(out::collect);
                    }
                });


        return null;
    }

    private static SingleOutputStreamOperator<Double> test2(SingleOutputStreamOperator<EventBean> watermarkStreams) {
      return   watermarkStreams.keyBy(EventBean::getWid).window(SlidingEventTimeWindows.of(Time.seconds(20),Time.seconds(10)))
                //用户个数，总时长
                .aggregate(new AggregateFunction<EventBean, Tuple2<Integer,Long>, Double>() {
            @Override
            public Tuple2<Integer, Long> createAccumulator() {
                return Tuple2.of(0,0L);
            }

            @Override
            public Tuple2<Integer, Long> add(EventBean value, Tuple2<Integer, Long> accumulator) {
                return Tuple2.of(accumulator.f0 +1,accumulator.f1 + value.getActTimeLong());
            }

            @Override
            public Double getResult(Tuple2<Integer, Long> accumulator) {
                return accumulator.f1 / (double) accumulator.f0;
            }

            @Override
            public Tuple2<Integer, Long> merge(Tuple2<Integer, Long> a, Tuple2<Integer, Long> b) {
                return Tuple2.of(a.f0+b.f0,a.f1+b.f1);
            }
        });
    }

    private static SingleOutputStreamOperator<Integer> test1(SingleOutputStreamOperator<EventBean> watermarkStreams) {
        SingleOutputStreamOperator<Integer> aggregate = watermarkStreams.keyBy(EventBean::getWid).window(SlidingEventTimeWindows.of(Time.seconds(20),Time.seconds(10)))
                //输入值，累加值，输出值
                .aggregate(new AggregateFunction<EventBean, Integer, Integer>() {
                    @Override
                    public Integer createAccumulator() {
                        return 0;
                    }

                    @Override
                    public Integer add(EventBean value, Integer accumulator) {
                        System.out.println("AggregateFunction add===="+value);
                        return accumulator + 1;
                    }

                    @Override
                    public Integer getResult(Integer accumulator) {
                        return accumulator;
                    }

                    @Override
                    public Integer merge(Integer a, Integer b) {
                        return a + b;
                    }
                });
        return aggregate;
    }
}
