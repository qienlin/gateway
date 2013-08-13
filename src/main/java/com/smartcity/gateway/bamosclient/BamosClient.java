package com.smartcity.gateway.bamosclient;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import net.sf.json.JSONObject;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpException;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.methods.StringRequestEntity;
import org.apache.log4j.Logger;

import com.smartcity.gateway.server.GatewayServer;

public class BamosClient {

	private static final Logger LOG = Logger.getLogger(BamosClient.class);

	private static final String BAMOS_URL = "bamos.url";

	private static String url;

	static {
		Properties properties = new Properties();
		try {
			properties.load(GatewayServer.class.getClassLoader().getResourceAsStream("config.properties"));
		} catch (IOException e) {
			LOG.error("Error reading config.properties", e);
		}
		url = properties.getProperty(BAMOS_URL);
	}

	public static void onCall(String serviceName, String channelId) {
		final JSONObject object = eventToJson("gateway", serviceName, "gateway", channelId);
		Executor executor = Executors.newSingleThreadExecutor();
		executor.execute(new Runnable() {
			@Override
			public void run() {
				HttpClient client = new HttpClient();
				PostMethod method = new PostMethod(url);
				try {
					method.setRequestEntity(new StringRequestEntity(object.toString(), "application/json", "UTF-8"));
					client.executeMethod(method);
				} catch (UnsupportedEncodingException e) {
					// ignore
				} catch (HttpException e) {
					// ignore
				} catch (IOException e) {
					// ignore
				}
			}
		});
	}

	private static JSONObject eventToJson(String serverName, String serviceName, String username, String channelId) {
		JSONObject responseData = new JSONObject();
		Map<String, Object> values = new HashMap<String, Object>();
		values.put("server", serverName);
		values.put("service", serviceName);
		values.put("username", username);
		values.put("timestamp", System.currentTimeMillis());
		values.put("channelID", channelId);
		responseData.element("event", values);
		return responseData;
	}

	public static void main(String[] args) {
		for (int i = 0; i < 10; i++)
			BamosClient.onCall("test", String.valueOf(i));
	}
}
