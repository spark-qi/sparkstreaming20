package com.qi.sparkstreaming_practice

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}
import org.apache.spark.{HashPartitioner, SparkConf}

import scala.collection.mutable

/**
  * Created by pc on 2018/1/26.
  */
object TerminateConnect {
  val conf = new SparkConf().setAppName("计算连接成功失败时长").setMaster("local[*]")
  val ssc = new StreamingContext(conf,Seconds(5))
  val kafkaParams = mutable.HashMap[String, Object]()
  kafkaParams.put("bootstrap.servers", "master:9092,slave1:9092")
  kafkaParams.put("key.deserializer", classOf[StringDeserializer])
  kafkaParams.put("value.deserializer", classOf[StringDeserializer])
  kafkaParams.put("group.id", "sparkstreaming")
  kafkaParams.put("auto.offset.reset", "latest")
  case class LogInfo(serialNum:String,terminatedId:String,time:String,online:String,serverIp:String)
  def calcIn10Min() = {
    //serialNum:1516775442880285,terminatedId:1505399,time:1516775549590,online:0,serverIp:192.168.171.231
    val dstreamFromKafka = KafkaUtils.createDirectStream(ssc,LocationStrategies.PreferConsistent
      ,ConsumerStrategies.Subscribe[String, String](List("SparkStreamingTest"),kafkaParams))

    val parserInfo = dstreamFromKafka.map(x=>{
      x.value().split(",") match {
        case Array(serialNum,terminatedId,time,online,serverIp) => LogInfo(serialNum.split(":")(1),terminatedId.split(":")(1),time.split(":")(1),online.split(":")(1),serverIp.split(":")(1))
        case _ => null
      }
    })
    //((terminatedId,serialNum),(是否成功连接，是否失败连接，连接时长))
    val result = parserInfo.map(x=>((x.terminatedId,x.serialNum),(x.time,x.online)))
        .combineByKey(
          (v:(String,String))=>{
            (if(v._2=="1") 1 else 0,if(v._2=="0") 1 else 0,v._1.toLong)
          }
          ,(c:(Int,Int,Long),v:(String,String))=>{
            (if(c._1==0 && v._2=="1") 1 else c._1
              ,if(c._2==0 && v._2=="0") 1 else c._2
              ,Math.abs(c._3-v._1.toLong)
              )
          }
          ,(c1:(Int,Int,Long),c2:(Int,Int,Long))=>{
            (if(c1._1==1 || c2._1==1) 1 else c1._1
              ,if(c1._2==1 || c2._2==1) 1 else c2._1
              ,Math.abs(c1._3-c2._3)
              )
          }
          ,new HashPartitioner(3)
        ).map(x=>(x._1._1,x._2)).reduceByKey((v1,v2)=>(v1._1+v2._1,v1._2+v2._2,v1._3+v2._3))
    result.print()//(terminatedId,(成功次数,失败次数,连接时长))
  }

  def main(args: Array[String]) {
    calcIn10Min()
    ssc.checkpoint("/ttt")
    ssc.start()
    ssc.awaitTermination()
  }
}
