package com.qi.sparkstreaming_practice

import java.util.{Date, Properties, UUID}

import kafka.message.Message
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.util.Random

/**
  * Created by ThinkPad on 2018/1/29.
  */
object TerminalRandom {
  val properties = new Properties()
  properties.setProperty("bootstrap.servers","master:9092,slave1:9092,slave2:9092")
  properties.setProperty("key.serializer","org.apache.kafka.common.serialization.StringSerializer")
  properties.setProperty("value.serializer","org.apache.kafka.common.serialization.StringSerializer")
  val producer = new KafkaProducer[String,String](properties)
  val terminatedIds = 1 to 100
  val random = new Random()
  val serverIp = "192.168.171.231"
  val topic = "forstreaming"

  def randomLog() = {
    val terminatedId = terminatedIds(random.nextInt(100))
    val serialNum = UUID.randomUUID()
    val time = new Date().getTime
    val randomNextTime = (random.nextInt(15)+1)*60*1000
    val endTime = time + randomNextTime

    val msg1 = s"serialNum:$serialNum,terminatedId:$terminatedId,time:$time,online:1,serverIp:$serverIp"
    val msg2 = s"serialNum:$serialNum,terminatedId:$terminatedId,time:$endTime,online:0,serverIp:$serverIp"
    println(msg1)
    println(msg2)
    (msg1,msg2)
  }
  def produceLog() = {
    val msgs = randomLog()
    val producerReocrd1 = new ProducerRecord[String,String](topic,msgs._1)
    val producerRecord2 = new ProducerRecord[String,String](topic,msgs._2)
    producer.send(producerReocrd1)
    producer.send(producerRecord2)
  }
  def main(args: Array[String]): Unit = {
   for (i<-1 to 10000){
     produceLog()
   }
    producer.flush()
    producer.close()
  }
}
