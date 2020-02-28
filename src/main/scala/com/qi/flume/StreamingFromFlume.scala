package com.qi.flume

import org.apache.spark.SparkConf
import org.apache.spark.streaming.flume.FlumeUtils
import org.apache.spark.streaming.{Duration, StreamingContext}

//flume push数据到sparkstreaming
object StreamingFromFlume {

  val conf=new SparkConf().setAppName("from flume push").setMaster("local[*]")
  val ssc=new StreamingContext(conf,Duration(3000))

  def getDStreamFromFlume(): Unit ={
    val receiveInputDStream=FlumeUtils.createStream(ssc,"192.168.6.124",9999)
//    x.event.getBody返回值为arraybuffer
    val flumeMsgDstream= receiveInputDStream.map(x=>new String(x.event.getBody.array()))
    flumeMsgDstream.print()
    ssc.start()
    ssc.awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    getDStreamFromFlume()
  }
}
