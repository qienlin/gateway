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

import java.net.InetSocketAddress;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

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

import com.smartcity.gateway.bootstrap.QueueConsumer;
import com.smartcity.gateway.handlers.GatewayHandler;

/**
 * @author (qienlin) Aug 9, 2013
 */
// @Configuration
public class GatewayServer {

	private final ConcurrentMap<Integer, Channel> connections = new ConcurrentHashMap<Integer, Channel>();

	private ServerBootstrap bootstrap;

	public void run(int port) {
		final Executor executor = new ThreadPoolExecutor(500, 500, 0L, TimeUnit.MILLISECONDS,
				new ArrayBlockingQueue<Runnable>(300), Executors.defaultThreadFactory(),
				new ThreadPoolExecutor.CallerRunsPolicy());
		bootstrap = new ServerBootstrap(new NioServerSocketChannelFactory(Executors.newCachedThreadPool(),
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
				pipeline.addLast("handler", new GatewayHandler(connections));
				return pipeline;
			}
		});
		bootstrap.setOption("child.KeepAlive", true);
		bootstrap.bind(new InetSocketAddress(port));

		QueueConsumer cm = new QueueConsumer("127.0.0.1", 5445, "jms.queue.sourceQueue");
		cm.start();
		cm.start();
		try {
			Thread.sleep(200000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		cm.close();
	}
}
