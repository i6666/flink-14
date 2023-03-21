package cn.jlili.hello

import org.apache.flink.streaming.api.datastream.DataStreamSource
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

object WindowWordCount {
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val text: DataStreamSource[String] = env.socketTextStream("localhost", 9999)


    env.execute("window word count")


    
  }
}
