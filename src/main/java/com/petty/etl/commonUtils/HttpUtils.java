package com.petty.etl.commonUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URLEncodedUtils;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.protocol.HTTP;
import org.apache.http.util.EntityUtils;

public class HttpUtils {
	private static HttpClient httpclient = null;
	static {
		httpclient = new DefaultHttpClient();
	}

	public static String postUrlResult(String url, String question, String answer) {
		// "http://192.168.1.20:9001/qaScore"
		HttpPost httpPost = new HttpPost(url);
		List<NameValuePair> postParameters = new ArrayList<NameValuePair>();
		StringBuilder result = new StringBuilder();

		try {
			postParameters.add(new BasicNameValuePair("q", question));
			postParameters.add(new BasicNameValuePair("a", answer));

			httpPost.setEntity(new UrlEncodedFormEntity(postParameters, "utf-8"));
			HttpResponse response = httpclient.execute(httpPost);

			try {
				HttpEntity entity = response.getEntity();

				// do something useful with the response body
				// and ensure it is fully consumed
				BufferedReader br = new BufferedReader(new InputStreamReader(entity.getContent()));
				String line;
				while ((line = br.readLine()) != null) {
					result.append(line + "\n");
				}
				EntityUtils.consume(entity);
			} catch (IOException e) {
				e.printStackTrace();
			}
		} catch (UnsupportedEncodingException e1) {
			e1.printStackTrace();
		} catch (ClientProtocolException e1) {
			e1.printStackTrace();
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		return result.toString();
	}

	public static String getUrlResult(String url, String question, String answer) {
		// "http://192.168.1.20:9001/qaScore?"
		StringBuilder result = new StringBuilder();
		URI uri = null;
		List<NameValuePair> qparams = new ArrayList<NameValuePair>();
		qparams.add(new BasicNameValuePair("q", question));
		qparams.add(new BasicNameValuePair("a", answer));
		String parameters = URLEncodedUtils.format(qparams, "UTF-8");
		try {
			uri = new URI(url + parameters);
			HttpGet httpGet = new HttpGet(uri);
			try {

				HttpResponse response = httpclient.execute(httpGet);

				try {
					HttpEntity entity = response.getEntity();

					// do something useful with the response body
					// and ensure it is fully consumed
					BufferedReader br = new BufferedReader(new InputStreamReader(entity.getContent()));
					String line;
					while ((line = br.readLine()) != null) {
						result.append(line + "\n");
					}
					EntityUtils.consume(entity);
				} catch (IOException e) {
					e.printStackTrace();
				}
			} catch (UnsupportedEncodingException e1) {
				e1.printStackTrace();
			} catch (ClientProtocolException e1) {
				e1.printStackTrace();
			} catch (IOException e1) {
				e1.printStackTrace();
			}
		} catch (URISyntaxException e2) {
			e2.printStackTrace();
		}
		return result.toString();
	}

	public static String postService(String posturl, String postData) {
		HttpPost httpPost = new HttpPost(posturl);
		StringBuilder result = new StringBuilder();

		try {
			StringEntity se = new StringEntity(postData, HTTP.UTF_8);
			se.setContentType("text/json");
			httpPost.setEntity(se);
			HttpResponse response = httpclient.execute(httpPost);
			try {
				HttpEntity entity = response.getEntity();
				BufferedReader br = new BufferedReader(new InputStreamReader(entity.getContent()));
				String line;
				while ((line = br.readLine()) != null) {
					result.append(line + "\n");
				}
				EntityUtils.consume(entity);
			} catch (IOException e) {
				e.printStackTrace();
			}
		} catch (UnsupportedEncodingException e1) {
			e1.printStackTrace();
		} catch (ClientProtocolException e1) {
			e1.printStackTrace();
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		return result.toString();
	}

	public static String callService(String url, String text, String key) {
		// "http://192.168.1.27:9005/json?"
		StringBuilder result = new StringBuilder();
		URI uri = null;
		List<NameValuePair> qparams = new ArrayList<NameValuePair>();
		qparams.add(new BasicNameValuePair(key, text));
		String parameters = URLEncodedUtils.format(qparams, "UTF-8");
		try {
			uri = new URI(url + "&" + parameters);
			HttpGet httpGet = new HttpGet(uri);
			try {

				HttpResponse response = httpclient.execute(httpGet);

				try {
					HttpEntity entity = response.getEntity();

					// do something useful with the response body
					// and ensure it is fully consumed
					BufferedReader br = new BufferedReader(new InputStreamReader(entity.getContent()));
					String line;
					while ((line = br.readLine()) != null) {
						result.append(line + "\n");
					}
					EntityUtils.consume(entity);
				} catch (IOException e) {
					e.printStackTrace();
				}
			} catch (UnsupportedEncodingException e1) {
				e1.printStackTrace();
			} catch (ClientProtocolException e1) {
				e1.printStackTrace();
			} catch (IOException e1) {
				e1.printStackTrace();
			}
		} catch (URISyntaxException e2) {
			e2.printStackTrace();
		}
		String string = result.toString();
		if(string.length() >= 1){
			string = string.substring(0, string.length() - 1);
		}
		return string;
	}
}
