package cc.mmail.hello.mapreduce.bean;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class SpeakOutBean implements Writable {

    public SpeakOutBean() {
    }

    private Long selfDuration;//自有内容时长(秒)
    private Long otherDuration;//第三⽅内容时长
    private Long sumDuration;//总内容时长
    private String devId;//日志id

    @Override
    public String toString() {
        return "SpeakOutBean{" +
                "selfDuration=" + selfDuration +
                ", otherDuration=" + otherDuration +
                ", sumDuration=" + sumDuration +
                ", devId='" + devId + '\'' +
                '}';
    }

    public Long getSelfDuration() {
        return selfDuration;
    }

    public void setSelfDuration(Long selfDuration) {
        this.selfDuration = selfDuration;
    }

    public Long getOtherDuration() {
        return otherDuration;
    }

    public void setOtherDuration(Long otherDuration) {
        this.otherDuration = otherDuration;
    }

    public Long getSumDuration() {
        return sumDuration;
    }

    public void setSumDuration(Long sumDuration) {
        this.sumDuration = sumDuration;
    }

    public void setSumDuration(Long otherDuration, Long selfDuration) {
        this.sumDuration = otherDuration + selfDuration;
    }

    public String getDevId() {
        return devId;
    }

    public void setDevId(String devId) {
        this.devId = devId;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(this.otherDuration);
        out.writeLong(this.selfDuration);
        out.writeBytes(this.devId);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.otherDuration = in.readLong();
        this.selfDuration = in.readLong();
        this.devId = in.readLine();
    }
}
