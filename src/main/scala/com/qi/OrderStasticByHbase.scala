package com.qi

import org.apache.hadoop.hbase.client.{ConnectionFactory, Get, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object OrderStasticByHbase {

  val conf = new SparkConf().setMaster("local[*]").setAppName("orderstasticByHbase")
  val ssc = new StreamingContext(conf, Seconds(5))

  def calcOrderStaticByHbase(): Unit = {
    val dstream = ssc.socketTextStream("slave1", 9999)
    val result = dstream.map(x => {
      val info = x.split("\\s+")
      info match {
        case Array(_, productId, accountNum, amountNum) => (productId, (accountNum.toInt, amountNum.toInt))
        case _ => null
      }
    }).filter(x => x != null).reduceByKey((x1, x2) => (x1._1 + x2._1, x1._2 + x2._2))


    result.foreachRDD(rdd => {

      rdd.foreach(x => {
        val conf = HBaseConfiguration.create()
        //Connection 的创建是个重量级的工作，线程安全，是操作hbase的入口
        val conn = ConnectionFactory.createConnection(conf)
        //从Connection获得 Admin 对象(相当于以前的 HAdmin)
        val admin = conn.getAdmin

        val userTable = TableName.valueOf("bd20:person_101")

        val table = conn.getTable(userTable)

        val get = new Get(x._1.getBytes)
        val result = table.get(get)

        var productId = x._1
        var accountNum = x._2._1
        var amountNum = x._2._2
        if (!result.isEmpty) {
          productId = x._1
          accountNum = x._2._1 + Bytes.toInt(result.getValue("i".getBytes, "accountNum".getBytes))
          amountNum = x._2._2 + Bytes.toInt(result.getValue("i".getBytes, "amountNum".getBytes))
        }

        val p = new Put(productId.getBytes())
        p.addColumn("i".getBytes, "productId".getBytes, productId.getBytes)
        p.addColumn("i".getBytes, "accountNum".getBytes, Bytes.toBytes(x._2._1))
        p.addColumn("i".getBytes, "amountNum".getBytes, Bytes.toBytes(x._2._2))

        table.put(p)
        table.close()
        conn.close()
      })

    })

    ssc.start()
    ssc.awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    calcOrderStaticByHbase()
  }
}
