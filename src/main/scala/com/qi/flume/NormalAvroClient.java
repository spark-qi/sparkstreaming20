package com.qi.flume;

import java.nio.charset.Charset;
import java.util.Date;

import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.api.RpcClient;
import org.apache.flume.api.RpcClientFactory;
import org.apache.flume.event.EventBuilder;


public class NormalAvroClient {

	private RpcClient client;
	private int port;
	private String hostname;
	
	public NormalAvroClient(int port,String hostname) {
		this.hostname=hostname;
		this.port=port;
		//初始化客户端
		client=RpcClientFactory.getDefaultInstance(hostname, port);
	}
	
	//发送消息
	public void sendMessage(String msg) {
		
		Event event=EventBuilder.withBody(msg,Charset.forName("utf-8"));
		try {
			client.append(event);
		} catch (EventDeliveryException e) {
			System.out.println("发送消息"+msg+"失败");
			e.printStackTrace();
		}
	}
	
	//关闭客户端
	public void close() {
		if (client!=null) {
			client.close();
		}
	}
	
	public static void main(String[] args) {
		
		NormalAvroClient teAvroClient=new NormalAvroClient(8888, "master");
		
		for (int i = 0; i < 20; i++) {
			teAvroClient.sendMessage(new Date().getTime()+"-msg"+i);
		}
		teAvroClient.close();
	}
	
}


























