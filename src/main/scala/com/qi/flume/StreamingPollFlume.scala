package com.qi.flume

import org.apache.spark.SparkConf
import org.apache.spark.streaming.flume.FlumeUtils
import org.apache.spark.streaming.{Duration, StreamingContext}

object StreamingPollFlume {

  val conf=new SparkConf().setAppName("from flume push").setMaster("local[*]")
  val ssc=new StreamingContext(conf,Duration(5000))

  def pollDataFromFlume()={
    val dstream=FlumeUtils.createPollingStream(ssc,"master",9999)
    dstream.map(x=>{
      new String(x.event.getBody.array())
    }).print()

    ssc.start()
    ssc.awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    pollDataFromFlume()
  }
}
