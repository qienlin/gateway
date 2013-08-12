package com.smartcity.gateway.bootstrap;

import java.util.Date;
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
import org.jboss.netty.buffer.DynamicChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.handler.codec.http.DefaultHttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.jboss.netty.handler.codec.http.HttpVersion;

public class QueueConsumer {

	private static final Logger LOG = Logger.getLogger(QueueConsumer.class);

	private ConcurrentMap<String, Channel> connections;

	private Properties properties;

	private ClientSessionFactory sessionFactory;

	public QueueConsumer(ClientSessionFactory sessionFactory, ConcurrentMap<String, Channel> connections,
			Properties properties) {
		this.sessionFactory = sessionFactory;
		this.connections = connections;
		this.properties = properties;
	}

	public void start() {
		for (int i = 0; i < Integer.valueOf(properties.getProperty("maxConsumerCount")); i++) {
			try {
				ClientSession session = sessionFactory.createSession();
				session.start();
				ClientConsumer consumer = session.createConsumer(properties.getProperty("targetQueue"));
				consumer.setMessageHandler(new MessageConsumerHandler(connections, session));
			} catch (HornetQException e) {
				LOG.error("Error initializing consumer", e);
			}
		}
	}

	private class MessageConsumerHandler implements MessageHandler {

		private ConcurrentMap<String, Channel> connections;
		private ClientSession session;

		public MessageConsumerHandler(ConcurrentMap<String, Channel> connections, ClientSession session) {
			this.connections = connections;
			this.session = session;
		}

		@Override
		public void onMessage(ClientMessage msg) {
			try {
				String connId = msg.getStringProperty("httpconnId");
				System.out.println(connId);
				Channel ch = connections.get(connId);
				HttpResponse response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
				ChannelBuffer buffer = new DynamicChannelBuffer(2048);
				buffer.writeBytes(("Hello World!\t" + new Date()).getBytes("UTF-8"));
				response.setContent(buffer);
				response.setHeader("Content-Type", "text/html; charset=UTF-8");
				response.setHeader("Content-Length", response.getContent().writerIndex());
				ch.write(response).addListener(ChannelFutureListener.CLOSE);
				// String httpversion = msg.getStringProperty("httpversion");
				// int status = msg.getIntProperty("httpstatus");
				// String headers = msg.getStringProperty("httpheaders");
				// byte[] content = msg.getBytesProperty("JMSCorrelationID");
				// ResponseBean rpbean = new ResponseBean();
				//
				// rpbean.setConnId(connId);
				// rpbean.setHttpversion(httpversion);
				// rpbean.setStatus(status);
				// rpbean.setHeaders(headers);
				// rpbean.setContent(content);
				msg.acknowledge();
				session.commit();
			} catch (HornetQException e) {
				e.printStackTrace();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
}
