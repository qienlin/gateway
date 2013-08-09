/*
 * FILE     :  ClientSessionPool.java
 *
 * CLASS    :  ClientSessionPool
 *
 * COPYRIGHT:
 *
 *   The computer systems, procedures, data bases and programs
 *   created and maintained by Qware Technology Group Co Ltd, are proprietary
 *   in nature and as such are confidential.  Any unauthorized
 *   use or disclosure of such information may result in civil
 *   liabilities.
 *
 *   Copyright 2013-8-7 by Qware Technology Group Co Ltd.
 *   All Rights Reserved.
 */
package com.smartcity.gateway.utils;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.apache.log4j.Logger;
import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.api.core.client.ClientSessionFactory;

/**
 * @author (huili) 2013-8-7
 */
public class ClientSessionPool {

	/**
	 * Logger of ClientSessionPool class.
	 */
	private static final Logger logger = Logger.getLogger(ClientSessionPool.class.getName());

	private final BlockingQueue<ClientSession> clientSessionQueue;

	public ClientSessionPool(final int maxSessionCount, final ClientSessionFactory sessionFactory) {
		clientSessionQueue = new ArrayBlockingQueue<ClientSession>(maxSessionCount);
		try {
			for (int i = 0; i < maxSessionCount; i++) {
				clientSessionQueue.add(sessionFactory.createSession());
				logger.info("Create session in pool:" + i);
			}
		} catch (HornetQException e) {
			logger.error("Create session failed.", e);
		}
	}

	/**
	 * Get an instance of client session in this pool if available
	 * 
	 * 
	 * @return an instance of the client session if available or null
	 */
	public ClientSession getSessionInstanceIfFree() {
		try {
			ClientSession result = clientSessionQueue.take();
			return result;
		} catch (InterruptedException e) {
			logger.error("Take session from pool failed.", e);
			return null;
		}
	}

	public boolean freeSessionInstance(ClientSession clientSession) {
		try {
			if (!clientSessionQueue.contains(clientSession)) {
				clientSessionQueue.put(clientSession);
				return true;
			}
			return false;
		} catch (InterruptedException e) {
			logger.error("Put session into pool failed.", e);
			return false;
		}
	}
}
