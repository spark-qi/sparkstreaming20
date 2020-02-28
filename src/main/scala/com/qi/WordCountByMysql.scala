package com.qi

import java.sql.DriverManager

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object WordCountByMysql {

  val conf=new SparkConf().setMaster("local[*]").setAppName("wordcount")
  val ssc=new StreamingContext(conf,Seconds(3))

  def calcuWordcount(): Unit ={
    val dstream=ssc.socketTextStream("slave1",9999)
    val result=dstream.flatMap(x=>x.split("\\s+")).map(x=>(x,1)).reduceByKey(_+_)

//    result.print()

    result.foreachRDD(rdd=>{
      rdd.foreach(x=>{
        Class.forName("com.mysql.jdbc.Driver")
        val connection=DriverManager.getConnection("jdbc:mysql://localhost:3306/sparktest","root","123456")
        println(x._1)
        val sql=s"select * from wordcount where word='${x._1}'"
        val stmt=connection.createStatement()
        val result=stmt.executeQuery(sql)
        var count=x._2
        var updateSql=s"insert into wordcount (word,count) values('${x._1}',${x._2})"
        if(result.next()){
          count+=result.getInt("count")
          updateSql=s"update wordcount set count=$count where word='${x._1}'"
        }
        stmt.execute(updateSql)
      })
    })
    ssc.start()
    ssc.awaitTermination()
  }


  def main(args: Array[String]): Unit = {

    calcuWordcount()
  }

}
