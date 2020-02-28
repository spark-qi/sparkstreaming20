package com.qi.sparkstreaming_practice


import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object DkafkatoStreaming {
  def main(args: Array[String]) {

    val sparkconf=new SparkConf().setAppName("project").setMaster("local")
    val ssc=new StreamingContext(sparkconf,Seconds(5))
    val kafkaParams = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG ->"master:9092",
      ConsumerConfig.GROUP_ID_CONFIG -> "test",
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer])
    ssc.checkpoint("hdfs://master:9000/data")
    val topics = Array("SparkStreamingTest")
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topics, kafkaParams))
    val lines=stream.map(_.value)
    val word=lines.flatMap(_.split(",")).map(x=>(x,1)).reduceByKey(_+_).print

    ssc.start()
    ssc.awaitTermination()
  }

}
