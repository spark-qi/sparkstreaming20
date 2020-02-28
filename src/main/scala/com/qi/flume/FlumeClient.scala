package com.qi.flume

import java.nio.charset.Charset
import java.util.Date

import org.apache.flume.api.{RpcClient, RpcClientFactory}
import org.apache.flume.event.EventBuilder

object FlumeClient {

  val client= RpcClientFactory.getDefaultInstance("master", 8888)

  def sendMessage(msg:String): Unit ={
    val event = EventBuilder.withBody(msg, Charset.forName("utf-8"))
    client.append(event)
  }

  def main(args: Array[String]): Unit = {
      for(i<-1 to 20){
        sendMessage(new Date().getTime+"msg-"+i)
      }
    client.close()
  }

}
