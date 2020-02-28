package com.qi

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Duration, Minutes, Seconds, StreamingContext}

object WindowWordCount {

  val conf=new SparkConf().setAppName("window count").setMaster("local[*]")
  val ssc=new StreamingContext(conf,Duration(3000))

  def wordCount(): Unit ={

    val dstream=ssc.socketTextStream("slave1",9999)
    val pairDstream=dstream.flatMap(_.split("\\s+")).map((_,1))
    //开窗计算
    //开窗聚合
    val result=pairDstream.reduceByKeyAndWindow((v1,v2)=>v1+v2,Minutes(1))

    result.print()

    ssc.start()
    ssc.awaitTermination()
  }

  //统计近一分钟之内的累计wordcount 每隔9秒输出一次结果,使用滑动宽度设置为9秒,窗口宽度是一分钟
  def wordCountSlider(): Unit ={
    val dstream=ssc.socketTextStream("slave1",9999)
    val pairDstream=dstream.flatMap(_.split("\\s+")).map((_,1))

    //开窗聚合,使用窗口滑动参数
    val result=pairDstream.reduceByKeyAndWindow((v1:Int,v2:Int)=>v1+v2,Minutes(1),Seconds(9))
    result.print()
    ssc.start()
    ssc.awaitTermination()
  }

  def main(args: Array[String]): Unit = {
//    wordCount()
    wordCountSlider()
  }
}
