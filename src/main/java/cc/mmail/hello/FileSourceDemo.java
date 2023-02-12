package cc.mmail.hello;

import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;

public class FileSourceDemo {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 8081);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        DataStreamSource<String> lines = env.readFile(new TextInputFormat(null), "src/main/resources/people.json", FileProcessingMode.PROCESS_CONTINUOUSLY, 2000);
        env.readTextFile("src/main/resources/people.json");
        lines.print();

        env.execute();


    }
}
