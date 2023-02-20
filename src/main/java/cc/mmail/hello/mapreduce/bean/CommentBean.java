package cc.mmail.hello.mapreduce.bean;

import org.apache.hadoop.io.WritableComparable;
import org.apache.http.client.utils.DateUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Date;

public class CommentBean implements WritableComparable<String> {

    private int id;
    private String comment;
    private int leve;
    private String phone;
    private String time;// 2019/4/9 10:35


    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(id);
        out.writeInt(leve);
        out.writeUTF(comment);
        out.writeUTF(phone);
        out.writeUTF(time);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        id = in.readInt();
        leve = in.readInt();
        comment = in.readUTF();
        phone = in.readUTF();
        time = in.readUTF();
    }

    String[] PATTERNS = new String[]{"yyyy/MM/dd HH:mm"};

    @Override
    public int compareTo(String o) {
        Date date1 = DateUtils.parseDate(time, PATTERNS);
        Date data2 = DateUtils.parseDate(o, PATTERNS);

        //时间倒序
        return -date1.compareTo(data2);
    }
}
