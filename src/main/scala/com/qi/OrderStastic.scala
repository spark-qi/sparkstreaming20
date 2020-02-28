package com.qi

import java.sql.DriverManager

import com.qi.TcpWordCount.ssc
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object OrderStastic {

  val conf=new SparkConf().setMaster("local[*]").setAppName("orderstastic")
  val ssc=new StreamingContext(conf,Seconds(3))

  def calcOrderStatic(): Unit ={
    val dstream=ssc.socketTextStream("slave1",9999)
    val result=dstream.map(x=>{
      val info=x.split("\\s+")
      info match {
        case Array(_,productId,accountNum,amountNum)=>(productId,(accountNum.toInt,amountNum.toInt))
        case _=>null
      }
    }).filter(x=>x!=null).reduceByKey((x1,x2)=>(x1._1+x2._1,x1._2+x2._2))

    result.foreachRDD(rdd=>{

      rdd.foreach(x=>{
        Class.forName("com.mysql.jdbc.Driver")
        val connection=DriverManager.getConnection("jdbc:mysql://localhost:3306/sparktest","root","123456")
        val sql=s"select * from order_stastic where product_id=${x._1}"
        val stmt=connection.createStatement()
        val result=stmt.executeQuery(sql)
        var accountNum=x._2._1
        var amountNum=x._2._2
        var updateSql=s"insert into order_stastic (product_id,account_num,amount_num) values(${x._1},$accountNum,$amountNum)"
        if(result.next()){
          accountNum+=result.getInt("account_num")
          amountNum+=result.getInt("amount_num")
          updateSql=s"update order_stastic set account_num=$accountNum,amount_num=$amountNum where product_id=${x._1}"
        }
        stmt.execute(updateSql)
      })
    })
    ssc.start()
    ssc.awaitTermination()
  }

  def calcOrderStasticWithState(): Unit ={
    val dstream=ssc.socketTextStream("slave1",9999)
    val result=dstream.map(x=>{
      val info=x.split("\\s+")
      info match {
        case Array(_,productId,accountNum,amountNum)=>(productId,(accountNum.toInt,amountNum.toInt))
        case _=>null
      }
    }).filter(x=>x!=null)
      .reduceByKey((x1,x2)=>(x1._1+x2._1,x1._2+x2._2))

    //使用状态计算必须得指定checkpoint路径,因为状态数据就存储在checkpoint里面
    ssc.checkpoint("file:///e:/aaa")
//    把累计的销量使用state来更新
//    调用updateStateByKey得到的dstream是状态的dstream
   val r= result.updateStateByKey((value:Seq[(Int,Int)],state:Option[(Int,Int)])=>{
       state match {
         case Some(s)=>if(value.size>0) Some(s._1+ value(0)._1,s._2+value(0)._2) else Some(s)
         case None=>Some(value(0))
       }
    })

    r.print()

    //不使用reduceByKey((x1,x2)=>(x1._1+x2._1,x1._2+x2._2))
//    val r2= result.updateStateByKey((values:Seq[(Int,Int)],state:Option[(Int,Int)])=>{
//      state match {
//        case Some(s)=>{
//          if(values.size>0){
//            val sumValues=values.reduce((v1,v2)=>(v1._1+v2._1,v1._2+v2._2))
//            Some((sumValues._1+s._1,sumValues._2+s._2))
//          }else{
//            Some(s)
//          }
//        }
//        case None=>{
//          val r=values.reduce((v1,v2)=>(v1._1+v2._1,v1._2+v2._2))
//          Some(r)
//        }
//      }
//    })
//
//    r2.print()

    ssc.start()
    ssc.awaitTermination()
  }

  def main(args: Array[String]): Unit = {

//    calcOrderStatic()
//    calcOrderStasticWithState()

  }

}
