package cc.mmail.hello.flow;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

public class BroadCastTest {
    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 8081);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);

            DataStreamSource<String> stream1 = env.socketTextStream("localhost", 9099);

        // id,eventId
        DataStream<Tuple2<String, String>> dataStream1 = stream1.map(new MapFunction<String, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> map(String value) throws Exception {
                String[] arr = value.split(",");
                return Tuple2.of(arr[0], arr[1]);
            }
        });
        DataStreamSource<String> stream2 = env.socketTextStream("localhost", 9098);

        //  id,age,city
        DataStream<Tuple3<String, String, String>> dataStream2 = stream2.map(new MapFunction<String, Tuple3<String, String, String>>() {
            @Override
            public Tuple3<String, String, String> map(String value) throws Exception {
                String[] arr = value.split(",");
                return Tuple3.of(arr[0], arr[1], arr[2]);
            }
        });

        //将流2 用户信息转换为广播流

        // new MapStateDescriptor<>("userInfoStateDesc", TypeInformation.of(String.class))
        MapStateDescriptor<String, Tuple2<String, String>> userInfoStateDesc = new MapStateDescriptor<String, Tuple2<String, String>>("userInfoStateDesc", TypeInformation.of(String.class), TypeInformation.of(new TypeHint<Tuple2<String, String>>() {
        })) {
        };
        BroadcastStream<Tuple3<String, String, String>> broadcast = dataStream2.broadcast(userInfoStateDesc);


        SingleOutputStreamOperator<String> result = dataStream1.connect(broadcast).process(new BroadcastProcessFunction<Tuple2<String, String>, Tuple3<String, String, String>, String>() {
            //处理主流中的数据 每来一条处理一条
            @Override
            public void processElement(Tuple2<String, String> value, BroadcastProcessFunction<Tuple2<String, String>, Tuple3<String, String, String>, String>.ReadOnlyContext ctx, Collector<String> out) throws Exception {
                ReadOnlyBroadcastState<String, Tuple2<String, String>> broadcastState = ctx.getBroadcastState(userInfoStateDesc);
                if (broadcastState != null) {
                    Tuple2<String, String> userInfo = broadcastState.get(value.f0);
                    out.collect(value.f0 + "," + value.f1 + ":" + (userInfo == null ? null : userInfo.f0) + "," + (userInfo == null ? null : userInfo.f1));
                } else {
                    out.collect(value.f0 + "," + value.f1 + ":" + null + "," + null);
                }
            }

            //处理广播流中的数据
            @Override
            public void processBroadcastElement(Tuple3<String, String, String> value, BroadcastProcessFunction<Tuple2<String, String>, Tuple3<String, String, String>, String>.Context ctx, Collector<String> out) throws Exception {
                BroadcastState<String, Tuple2<String, String>> broadcastState = ctx.getBroadcastState(userInfoStateDesc);
                System.out.println("广播流发生变动 processBroadcastElement " + value);
                broadcastState.put(value.f0, Tuple2.of(value.f1, value.f2));
            }
        });

        result.print();

        env.execute();

    }
}
