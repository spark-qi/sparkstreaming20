package com.qi

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Duration, StreamingContext}

object TcpWordCount {

  val conf=new SparkConf().setAppName("tcp wc").setMaster("local[*]")
  val ssc=new StreamingContext(conf,Duration(15000))

  //计算流wordcount
  def main(args: Array[String]): Unit = {
    //加载流数据DStream
    val dstream=ssc.socketTextStream("slave1",9999)
    //对流数据DStream调用api方法完成数据处理逻辑
    val result=dstream.flatMap(x=>x.split("\\s+")).map(x=>(x,1)).reduceByKey(_+_)

    //action打印出前20条数据
    result.print(20)
    //启动流计算
    ssc.start()
    ssc.awaitTermination()

  }
}
