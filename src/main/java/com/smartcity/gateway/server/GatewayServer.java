/*
 * FILE     :  GatewayServer.java
 *
 * CLASS    :  GatewayServer
 *
 * COPYRIGHT:
 *
 *   The computer systems, procedures, data bases and programs
 *   created and maintained by Qware Technology Group Co Ltd, are proprietary
 *   in nature and as such are confidential.  Any unauthorized
 *   use or disclosure of such information may result in civil
 *   liabilities.
 *
 *   Copyright Aug 9, 2013 by Qware Technology Group Co Ltd.
 *   All Rights Reserved.
 */
package com.smartcity.gateway.server;

import static org.jboss.netty.channel.Channels.pipeline;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.client.ClientSessionFactory;
import org.hornetq.api.core.client.HornetQClient;
import org.hornetq.core.remoting.impl.netty.NettyConnectorFactory;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.handler.codec.http.HttpChunkAggregator;
import org.jboss.netty.handler.codec.http.HttpRequestDecoder;
import org.jboss.netty.handler.codec.http.HttpResponseEncoder;
import org.jboss.netty.handler.execution.ExecutionHandler;
import org.jboss.netty.handler.stream.ChunkedWriteHandler;

import com.smartcity.gateway.handlers.GatewayHandler;
import com.smartcity.gateway.handlers.MessageConsumer;
import com.smartcity.gateway.handlers.MessageProducer;

/**
 * @author (qienlin) Aug 9, 2013
 */
// @Configuration
public class GatewayServer {

	private static final Logger LOG = Logger.getLogger(GatewayServer.class);

	private static final String SERVER_PORT = "server.port";

	private static final String HORNETQ_HOST = "hornetq.host";

	private static final String HORNETQ_PORT = "hornetq.port";

	/**
	 * The property file
	 */
	private Properties properties;

	/**
	 * All the established connections
	 */
	private final ConcurrentMap<String, Channel> connections = new ConcurrentHashMap<String, Channel>();

	/**
	 * The client session factory for creating sessions
	 */
	private ClientSessionFactory sessionFactory;

	/**
	 * Initiate context including loading {@code Properties} file and
	 * instantiate the {@code ClientSessionFactory}.
	 */
	private void init() {
		properties = new Properties();
		try {
			properties.load(GatewayServer.class.getClassLoader().getResourceAsStream("config.properties"));
		} catch (IOException e) {
			LOG.error("Error reading config.properties", e);
		}
		Map<String, Object> paramMap = new HashMap<String, Object>();
		paramMap.put("host", properties.getProperty(HORNETQ_HOST));
		paramMap.put("port", properties.getProperty(HORNETQ_PORT));
		try {
			sessionFactory = HornetQClient.createServerLocatorWithHA(
					new TransportConfiguration(NettyConnectorFactory.class.getName(), paramMap)).createSessionFactory();
		} catch (Exception e) {
			LOG.error("Error initializing ClientSessionFactory", e);
		}
	}

	/**
	 * A method to instantiate {@code ServerBootstrap} and listen on the port
	 * given.
	 * 
	 */
	public void run() {
		init();
		final MessageProducer producer = new MessageProducer(sessionFactory, properties);
		// consumer initialization
		MessageConsumer consumer = new MessageConsumer(sessionFactory, connections, properties);
		consumer.start();

		final Executor executor = new ThreadPoolExecutor(500, 500, 0L, TimeUnit.MILLISECONDS,
				new ArrayBlockingQueue<Runnable>(300), Executors.defaultThreadFactory(),
				new ThreadPoolExecutor.CallerRunsPolicy());
		ServerBootstrap bootstrap = new ServerBootstrap(new NioServerSocketChannelFactory(executor,
				Executors.newCachedThreadPool()));
		bootstrap.setPipelineFactory(new ChannelPipelineFactory() {
			@Override
			public ChannelPipeline getPipeline() throws Exception {
				ChannelPipeline pipeline = pipeline();
				pipeline.addLast("decoder", new HttpRequestDecoder());
				pipeline.addLast("aggregator", new HttpChunkAggregator(Integer.MAX_VALUE));
				pipeline.addLast("encoder", new HttpResponseEncoder());
				pipeline.addLast("chunked", new ChunkedWriteHandler());
				pipeline.addLast("pipelineExecutor", new ExecutionHandler(executor));
				pipeline.addLast("handler", new GatewayHandler(connections, producer));
				return pipeline;
			}
		});
		bootstrap.setOption("child.KeepAlive", true);
		bootstrap.bind(new InetSocketAddress(Integer.valueOf(properties.getProperty(SERVER_PORT))));
	}
}
