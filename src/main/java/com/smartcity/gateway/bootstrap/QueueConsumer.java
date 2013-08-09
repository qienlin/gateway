package com.smartcity.gateway.bootstrap;

import java.util.HashMap;
import java.util.Map;

import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.client.ClientConsumer;
import org.hornetq.api.core.client.ClientMessage;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.api.core.client.ClientSessionFactory;
import org.hornetq.api.core.client.HornetQClient;
import org.hornetq.api.core.client.MessageHandler;
import org.hornetq.api.core.client.ServerLocator;
import org.hornetq.core.remoting.impl.netty.NettyConnectorFactory;

public class QueueConsumer{
	
	private ServerLocator locator;
	private ClientSessionFactory sessionfactory;
	private ClientSession session;
	private ClientConsumer consumer;
	public QueueConsumer(String host,int port,String queueName) {
		Map<String, Object> connParams = new HashMap<String, Object>();
		connParams.put("host", host);
		connParams.put("port", port);
//		HqMsgHandler handler = new HqMsgHandler();
		locator = HornetQClient
				.createServerLocatorWithHA(new TransportConfiguration(
						NettyConnectorFactory.class.getName(), connParams));
		try {
			sessionfactory = locator.createSessionFactory();
			session = sessionfactory.createTransactedSession();
			consumer = session.createConsumer(queueName);
			//consumer.setMessageHandler(handler);
			consumer.setMessageHandler(new MessageHandler(){

				public void onMessage(ClientMessage msg) {
					System.out.println(msg.getObjectProperty("data"));
					try {
						System.out.println(msg.toString());
						String connId = msg.getStringProperty("httpconnId");
						String httpversion = msg.getStringProperty("httpversion");
						int status = msg.getIntProperty("httpstatus");
						String headers = msg.getStringProperty("httpheaders");
						byte [] content = msg.getBytesProperty("JMSCorrelationID");;
//						ResponseBean rpbean = new ResponseBean();
//						
//						rpbean.setConnId(connId);
//						rpbean.setHttpversion(httpversion);
//						rpbean.setStatus(status);
//						rpbean.setHeaders(headers);
//						rpbean.setContent(content);
						msg.acknowledge();
						session.commit();
					} catch (HornetQException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
				
			});
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
    
	public void start(){
		try {
			session.start();
		} catch (HornetQException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void close(){
		try{
			if(session != null)session.close();
			if(sessionfactory != null) sessionfactory.close();
			if(locator != null) locator.close();
		}catch(Exception e){
			e.printStackTrace();
		}
	}
}
