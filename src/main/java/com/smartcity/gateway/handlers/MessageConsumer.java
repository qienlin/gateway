package com.smartcity.gateway.handlers;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentMap;

import org.apache.log4j.Logger;
import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.client.ClientConsumer;
import org.hornetq.api.core.client.ClientMessage;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.api.core.client.ClientSessionFactory;
import org.hornetq.api.core.client.MessageHandler;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.handler.codec.http.DefaultHttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.jboss.netty.handler.codec.http.HttpVersion;

import com.google.gson.Gson;

public class MessageConsumer {

	private static final Logger LOG = Logger.getLogger(MessageConsumer.class);
	
	private static final String MAX_CONSUMER_COUNT = "maxConsumerCount";
	
	private static final String TARGET_QUEUE = "targetQueue";

	/**
	 * All the connections on HTTP connection established
	 */
	private ConcurrentMap<String, Channel> connections;
	
	/**
	 * Property file
	 */
	private Properties properties;

	/**
	 * Client session factory for creating session
	 */
	private ClientSessionFactory sessionFactory;

	/**
	 * Constructor
	 * 
	 * @param sessionFactory
	 * @param connections
	 * @param properties
	 */
	public MessageConsumer(ClientSessionFactory sessionFactory, ConcurrentMap<String, Channel> connections,
			Properties properties) {
		this.sessionFactory = sessionFactory;
		this.connections = connections;
		this.properties = properties;
	}

	/**
	 * Start to listen on the message from Mule.
	 * The number of consumer is in customization.
	 */
	public void start() {
		for (int i = 0; i < Integer.valueOf(properties.getProperty(MAX_CONSUMER_COUNT)); i++) {
			try {
				ClientSession session = sessionFactory.createSession();
				session.start();
				ClientConsumer consumer = session.createConsumer(properties.getProperty(TARGET_QUEUE));
				consumer.setMessageHandler(new MessageConsumerHandler(connections, session));
			} catch (HornetQException e) {
				LOG.error("Error initializing consumer", e);
			}
		}
	}

	/**
	 * Inner class for handling message received from Mule.
	 * It implements onMessage method where response is built 
	 * according to the message received.
	 * 
	 */
	private class MessageConsumerHandler implements MessageHandler {

		private ConcurrentMap<String, Channel> connections;
		
		private ClientSession session;

		/**
		 * Constructor
		 * 
		 * @param connections
		 * @param session
		 */
		public MessageConsumerHandler(ConcurrentMap<String, Channel> connections, ClientSession session) {
			this.connections = connections;
			this.session = session;
		}

		/* (non-Javadoc)
		 * @see org.hornetq.api.core.client.MessageHandler#onMessage(org.hornetq.api.core.client.ClientMessage)
		 */
		@Override
		public void onMessage(ClientMessage message) {
			try {
				sendResponse(message);
				message.acknowledge();
				session.commit();
			} catch (HornetQException e) {
				LOG.error("Error acknowledging message or committing session", e);
			} 
		}
		
		/**
		 * Send back response to the client side according to 
		 * received message from Mule
		 * 
		 * @param message
		 * 			the JMS message received
		 */
		private void sendResponse(ClientMessage message){
			String connId = message.getStringProperty("httpconnId");
			LOG.debug("Receiving message. Channel Id: " + connId);
			Channel ch = connections.get(connId);
			HttpResponse response = new DefaultHttpResponse(HttpVersion.valueOf(message.getStringProperty("httpversion")), HttpResponseStatus.OK);
			response.setHeader("Content-Type", "text/html; charset=UTF-8");
			@SuppressWarnings("unchecked")
			Map<String,String> headerMap = new Gson().fromJson(message.getStringProperty("httpheaders"), Map.class);
			for(Map.Entry<String, String> entry:headerMap.entrySet()){
				response.setHeader(entry.getKey(), entry.getValue());
			} 
			int contentLength = message.getBytesProperty("httpcontent").length;
			response.setHeader("Content-Length", contentLength);
			ChannelBuffer buffer = ChannelBuffers.dynamicBuffer( contentLength);
			buffer.writeBytes(message.getBytesProperty("httpcontent"));
			response.setContent(buffer);
			ch.write(response).addListener(ChannelFutureListener.CLOSE);
		}
	}
}
