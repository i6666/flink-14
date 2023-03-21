package cc.mmail.hello.flume;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;

public class MyInterceptor implements Interceptor {
    @Override
    public void initialize() {

    }

    @Override
    public Event intercept(Event event) {
        byte[] body = event.getBody();
        Map<String, String> headers = event.getHeaders();


        String logType = headers.get("logtype");
        try {
            if (body == null || body.length == 0 || logType == null) {
                return event;
            }
            String bodyStr = new String(body);
            Long timestamp ;
            if (logType.equals("event")) {
                String json = bodyStr.split("AppEvent -")[1];
                JSONObject jsonObject = JSON.parseObject(json);
                timestamp = jsonObject.getJSONArray("lagou_event").getJSONObject(0).getLong("time");
            } else if (logType.equals("start")) {
                String json = bodyStr.split("AppStart -")[1];
                JSONObject jsonObject = JSON.parseObject(json);
                timestamp = jsonObject.getJSONObject("app_active").getLong("time");
            }else {
                throw new RuntimeException();
            }

            // 将字符串转换为Long
            DateTimeFormatter formatter =
                    DateTimeFormatter.ofPattern("yyyy-MM-dd");
            Instant instant =
                    Instant.ofEpochMilli(timestamp);
            LocalDateTime localDateTime =
                    LocalDateTime.ofInstant(instant, ZoneId.systemDefault());
            String date = formatter.format(localDateTime);
            headers.put("logtime", date);
        } catch (Exception e) {
            headers.put("logtime", "unknow");
        }
        return event;
    }

    @Override
    public List<Event> intercept(List<Event> list) {
        if (list != null) {
            list.forEach(this::intercept);
        }
        return list;
    }

    @Override
    public void close() {

    }

    public static class Builder implements Interceptor.Builder {

        @Override
        public Interceptor build() {
            return new MyInterceptor();
        }

        @Override
        public void configure(Context context) {

        }
    }


}
