package com.qi.sparkstreaming_practice

import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object TerminateConnectAnalysis {

  val conf=new SparkConf().setAppName("poll from kafka").setMaster("local[*]")
  val ssc=new StreamingContext(conf,Seconds(10))

  val kafkaParams=Map(
    "bootstrap.servers"->"master:9092,slave1:9092,slave2:9092",
    "key.deserializer"->"org.apache.kafka.common.serialization.StringDeserializer",
    "value.deserializer"->"org.apache.kafka.common.serialization.StringDeserializer",
    "group.id"->"sparkstreaming"
  )

  def getStreamFromKafka(): Unit ={
    val kafkaStream=KafkaUtils.createDirectStream(ssc,LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String,String](List("forstreaming"),kafkaParams))
    kafkaStream.map(x=>{
      val regex = ".*?:(.*?),.*?:(.*?),.*?:(.*?),.*?:(.*?),.*?:(.*?)".r
      x.value() match {
        case regex(serialNum,terminatedId,time,online,_) =>(terminatedId,(serialNum,time,online))
        case _=>null
      }
    }).reduceByKey((x1,x2)=>{
    x1

    })

    ssc.start()
    ssc.awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    getStreamFromKafka()
  }

}
