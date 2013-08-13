package com.smartcity.gateway.handlers;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.client.ClientMessage;
import org.hornetq.api.core.client.ClientProducer;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.api.core.client.ClientSessionFactory;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpRequest;

import com.google.gson.Gson;
import com.smartcity.gateway.utils.ClientSessionPool;

public class MessageProducer {

	private static final Logger LOG = Logger.getLogger(MessageProducer.class);

	private static final String MAX_SESSION_COUNT = "maxSessionCount";

	private static final String SOURCE_QUEUE = "sourceQueue";

	/**
	 * The property file
	 */
	private Properties properties;

	/**
	 * The client session pool
	 */
	private ClientSessionPool pool;

	/**
	 * Constructor
	 * 
	 * @param sessionFactory
	 * @param properties
	 */
	public MessageProducer(ClientSessionFactory sessionFactory, Properties properties) {
		this.properties = properties;
		this.pool = new ClientSessionPool(Integer.valueOf(properties.getProperty(MAX_SESSION_COUNT)), sessionFactory);
	}

	/**
	 * Wrap up the received HTTP request and send it to the HornetQ
	 * 
	 * @param channelId
	 * @param request
	 */
	public void sendMessage(String channelId, HttpRequest request) {
		ClientSession session = pool.getSessionInstanceIfFree();
		ClientMessage message = session.createMessage(true);
		message.putStringProperty("httpconnId", channelId);
		message.putStringProperty("httpversion", request.getProtocolVersion().getText());
		message.putStringProperty("httpmethod", request.getMethod().getName());
		message.putStringProperty("httpurl", request.getUri());
		Map<String, String> headerMap = new HashMap<String, String>();
		for (Map.Entry<String, String> header : request.getHeaders()) {
			headerMap.put(header.getKey(), header.getValue());
		}
		headerMap.put("userName", request.getHeader("Authorization"));
		message.putStringProperty("httpheaders", new Gson().toJson(headerMap));
		ChannelBuffer buffer = ChannelBuffers.dynamicBuffer((int) HttpHeaders.getContentLength(request));
		buffer.writeBytes(request.getContent());
		message.putBytesProperty("httpcontent", buffer.array());
		try {
			ClientProducer producer = session.createProducer(properties.getProperty(SOURCE_QUEUE));
			LOG.debug("Sending message. Channel Id: " + channelId);
			System.err.println(channelId);
			producer.send(message);
			producer.close();
			pool.freeSessionInstance(session);
		} catch (HornetQException e) {
			LOG.error("Error creating ClientProducer or sending message", e);
		}
	}
}
