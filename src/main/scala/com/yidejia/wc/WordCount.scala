package com.yidejia.wc

//需要导入隐式转换

import org.apache.flink.api.scala._

object WordCount {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds: DataSet[String] = env.readTextFile("E:\\software\\projects\\flink\\src\\main\\resources\\wc.txt")
    val resultDS: AggregateDataSet[(String, Int)] = ds.flatMap(_.split(" "))
      .map((_, 1)).
      groupBy(0).
      sum(1)

    resultDS.print()
  }

}
