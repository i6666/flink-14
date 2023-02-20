package cc.mmail.hello.mapreduce.bean;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class UserBean implements Writable {
    private long userId;
    private long positionId;
    private String date;
    private long id;
    private String positionName;
    private String flag;


    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(this.userId);
        out.writeLong(this.positionId);
        out.writeBytes(date);
        out.writeLong(this.id);
        out.writeBytes(positionName);
        out.writeBytes(flag);

    }

    public UserBean() {
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.userId = in.readLong();
        this.positionId = in.readLong();
        this.date = String.valueOf(in.readByte());
        this.id = in.readLong();
        this.positionName =  String.valueOf(in.readByte());
        this.flag =  String.valueOf(in.readByte());
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public String getPositionName() {
        return positionName;
    }

    public void setPositionName(String positionName) {
        this.positionName = positionName;
    }

    public String getFlag() {
        return flag;
    }

    public void setFlag(String flag) {
        this.flag = flag;
    }

    public long getUserId() {
        return userId;
    }

    public void setUserId(long userId) {
        this.userId = userId;
    }

    public long getPositionId() {
        return positionId;
    }

    public void setPositionId(long positionId) {
        this.positionId = positionId;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }
}
