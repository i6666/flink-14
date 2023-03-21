package cc.mmail.hello.udf;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class UDFJsonArray extends UDF {

    public UDFJsonArray() {
    }

    public ArrayList<String> evaluate(String json, String arrayKey) {

        try {
            if (json == null) {
                return null;
            }
            JSONObject  object = JSONObject.parseObject(json);
            JSONArray jsonArray = object.getJSONArray(arrayKey);
            ArrayList<String> list = new ArrayList<>();
            if (jsonArray != null) {
                for (Object o : jsonArray) {
                    list.add(o.toString());
                }
            }
            return list;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }

    }

    @Test
    public void test() {
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("id",1);
        ArrayList<Integer> integers = Lists.newArrayList(101, 102, 103);
        jsonObject.put("ids",integers
            );

        System.out.println(jsonObject.toString());

        String str = "{\"id\": 1,\"ids\": [101,102,103],\"total_number\": 3}";
        List<String> ids = evaluate("{\"id\": 1,\"ids\": [101,102,103],\"total_number\": 3}", "ids");
        System.out.println(ids);
    }


}
