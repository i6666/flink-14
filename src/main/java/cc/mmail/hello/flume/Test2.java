package cc.mmail.hello.flume;

import org.apache.flume.Event;
import org.apache.flume.event.SimpleEvent;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Test2 {

    @Test
    public void test(){
        SimpleEvent simpleEvent = new SimpleEvent();
        String  str = "2020-08-20 11:56:09.115 [main] INFO  com.lagou.ecommerce.AppStart - {\"app_active\":{\"name\":\"app_active\",\"json\":{\"entry\":\"2\",\"action\":\"1\",\"error_code\":\"0\"\n" +
                "},\"time\":1595410795265},\"attr\":{\"area\":\"阳江\",\"uid\":\"2F10092A6003\",\"app_v\":\"1.1.2\",\"event_type\":\"common\",\"device_id\":\"1FB872-9A1006003\",\"os_type\":\"0.86\"\n" +
                ",\"channel\":\"BD\",\"language\":\"chinese\",\"brand\":\"xiaomi-2\"}}";

        str = "2020-08-20 12:00:58.395 [main] INFO  com.lagou.ecommerce.AppEvent -"+"{\"lagou_event\":[{\"name\":\"goods_detail_loading\",\"json\":{\"entry\":\"2\",\"goodsid\":\"0\",\"loading_time\":\"92\",\"action\":\"3\",\"staytime\":\"10\",\"showtype\":\"0\"},\"time\":1595265099584},{\"name\":\"notification\",\"json\":{\"action\":\"1\",\"type\":\"3\"},\"time\":1595341087663},{\"name\":\"ad\",\"json\":{\"duration\":\"10\",\"ad_action\":\"0\",\"shop_id\":\"23\",\"event_type\":\"ad\",\"ad_type\":\"1\",\"show_style\":\"0\",\"product_id\":\"36\",\"place\"\n" +
                ":\"placecampaign2_left\",\"sort\":\"1\"},\"time\":1595276738208}],\"attr\":{\"area\":\"东莞\",\"uid\":\"2F10092A0\",\"app_v\":\"1.1.0\",\"event_type\":\"common\",\"device_id\":\"1FB872-9A1000\",\"os_type\":\"1.1\",\"channel\":\"AD\",\"language\":\"chinese\",\"brand\":\"iphone-0\"}}";
        Map<String, String> map = new HashMap<>();
        map.put("logtype","start");
        map.put("logtype","event");
        simpleEvent.setHeaders(map);
        simpleEvent.setBody(str.getBytes(StandardCharsets.UTF_8));
        MyInterceptor myInterceptor = new MyInterceptor();
        ArrayList<Event> objects = new ArrayList<>();
        objects.add(simpleEvent);
        List<Event> intercept = myInterceptor.intercept(objects);
        System.out.println(intercept);

    }

}
