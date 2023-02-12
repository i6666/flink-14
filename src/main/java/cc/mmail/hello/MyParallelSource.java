package cc.mmail.hello;

import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

public class MyParallelSource extends RichParallelSourceFunction<String> {
    private boolean flag = true;



    private int i = 1;
    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        while (i < 100 && flag){
            Thread.sleep(10);
            ctx.collect("data"+i);
        }
    }

    @Override
    public void cancel() {
        flag = false;
    }
}
