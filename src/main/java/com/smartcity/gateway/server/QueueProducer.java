package com.smartcity.gateway.server;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.client.ClientMessage;
import org.hornetq.api.core.client.ClientProducer;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.api.core.client.HornetQClient;
import org.hornetq.core.remoting.impl.netty.NettyConnectorFactory;

import com.smartcity.gateway.utils.ClientSessionPool;

public class QueueProducer {

	private static final Logger LOG = Logger.getLogger(QueueProducer.class);

	private static Properties properties;

	private static ClientSessionPool pool;

	static {
		properties = new Properties();
		try {
			properties.load(QueueProducer.class.getClassLoader().getResourceAsStream("config.properties"));
		} catch (IOException e) {
			e.printStackTrace();
			LOG.error("Read config.properties error", e);
		}
		Map<String, Object> connParams = new HashMap<String, Object>();
		connParams.put("host", properties.getProperty("host"));
		connParams.put("port", properties.getProperty("port"));
		try {
			pool = new ClientSessionPool(500, HornetQClient.createServerLocatorWithHA(
					new TransportConfiguration(NettyConnectorFactory.class.getName(), connParams))
					.createSessionFactory());

		} catch (Exception e) {
			e.printStackTrace();
			LOG.error("Error creating client session Factory", e);
		}
	}

	public static void sendMessage(Object message) {
		ClientSession session = pool.getSessionInstanceIfFree();
		ClientMessage msg = session.createMessage(true);
		msg.putObjectProperty("data", message);
		try {
			ClientProducer producer = session.createProducer(properties.getProperty("sourceQueue"));
			producer.send(msg);
			producer.close();
			pool.freeSessionInstance(session);
		} catch (HornetQException e) {
			e.printStackTrace();
			LOG.error("Error creating ClientProducer or sending message", e);
		}
	}
}
