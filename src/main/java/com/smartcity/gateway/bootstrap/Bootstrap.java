/*
 * FILE     :  Bootstrap.java
 *
 * CLASS    :  Bootstrap
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
package com.smartcity.gateway.bootstrap;

import com.smartcity.gateway.server.GatewayServer;

/**
 * @author (qienlin) Aug 9, 2013
 */
public class Bootstrap {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// @SuppressWarnings("resource")
		// ApplicationContext context = new
		// ClassPathXmlApplicationContext("classpath:applicationContext.xml");
		// GatewayServer server = context.getBean(GatewayServer.class);
		new GatewayServer().run(8080);
		
		
	}
}
