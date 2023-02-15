package cc.mmail.hello.mapreduce;

import cc.mmail.hello.mapreduce.bean.SpeakOutBean;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SpeakDurationReducer extends Reducer<Text, SpeakOutBean, Text, SpeakOutBean> {

    SpeakOutBean outBean = new SpeakOutBean();

    @Override
    protected void reduce(Text key, Iterable<SpeakOutBean> values, Reducer<Text, SpeakOutBean, Text, SpeakOutBean>.Context context) throws IOException, InterruptedException {
        long selfDuration = 0;//自有内容时长(秒)
        long otherDuration = 0;//第三⽅内容时长

        for (SpeakOutBean value : values) {
            selfDuration += value.getSelfDuration();
            otherDuration += value.getOtherDuration();
        }
        outBean.setSelfDuration(selfDuration);
        outBean.setOtherDuration(otherDuration);
        outBean.setSumDuration(otherDuration,selfDuration);
        context.write(key, outBean);
    }
}
