package com.qi.kafka


import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object StreamingFromKafka {

  val conf=new SparkConf().setAppName("poll from kafka").setMaster("local[*]")
  val ssc=new StreamingContext(conf,Seconds(10))

  val kafkaParams=Map(
    "bootstrap.servers"->"master:9092",
    "key.deserializer"->"org.apache.kafka.common.serialization.StringDeserializer",
    "value.deserializer"->"org.apache.kafka.common.serialization.StringDeserializer",
    "group.id"->"test"
  )
  ssc.checkpoint("hdfs://master:9000/data")
  def getStreamFromKafka(): Unit ={
    val kafkaStream=KafkaUtils.createDirectStream(ssc,LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String,String](List("SparkStreamingTest"),kafkaParams))
    kafkaStream.map(x=>(x.key(),x.value())).print()

    ssc.start()
    ssc.awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    getStreamFromKafka()
  }

}
