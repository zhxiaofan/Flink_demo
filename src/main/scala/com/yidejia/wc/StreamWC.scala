package com.yidejia.wc

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

object StreamWC {
  def main(args: Array[String]): Unit = {
    val params: ParameterTool = ParameterTool.fromArgs(args)
    val hostname: String = params.get("hostname")
    val port: Int = params.getInt("port")


    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val wordsDS: DataStream[String] = env.socketTextStream(hostname, port)
    // DataStream 聚合操作使用key by . DataSet使用group by
    val wcDS: DataStream[(String, Int)] = wordsDS.flatMap(_.split(" ")).map((_, 1)).keyBy(0).sum(1)

    wcDS.print().setParallelism(1)


  }
}
