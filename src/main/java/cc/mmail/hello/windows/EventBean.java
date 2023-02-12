package cc.mmail.hello.windows;

import java.io.Serializable;

public class EventBean implements Serializable {

    private Long  wid;

    /**
     * 行为时长
     */
    private Long actTimeLong;

    private Long eventTime;



    @Override
    public String toString() {
        return "EventBean{" +
                "wid=" + wid +
                ", actTimeLong=" + actTimeLong +
                ", eventTime=" + eventTime +
                '}';
    }

    public Long getEventTime() {
        return eventTime;
    }

    public void setEventTime(Long eventTime) {
        this.eventTime = eventTime;
    }

    public EventBean(Long wid, Long actTimeLong, Long eventTime) {
        this.wid = wid;
        this.actTimeLong = actTimeLong;
        this.eventTime = eventTime;
    }

    public Long getWid() {
        return wid;
    }

    public void setWid(Long wid) {
        this.wid = wid;
    }

    public Long getActTimeLong() {
        return actTimeLong;
    }

    public void setActTimeLong(Long actTimeLong) {
        this.actTimeLong = actTimeLong;
    }
}
