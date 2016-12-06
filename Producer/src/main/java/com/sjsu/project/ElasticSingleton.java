package com.sjsu.project;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Properties;

import org.elasticsearch.index.query.TermQueryBuilder;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.TimeValue;

import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;

import org.ini4j.Ini;

@SuppressWarnings("unused")
public class ElasticSingleton  extends Thread{

	public static Client client;
	public static String ES_IP;
	/* producer properties */
	public static Properties properties;
	public static String indexName;
	public static String nodeName;
	public static String kafkaServer;
	public static String time;
	public static String numProducer;
	public static ArrayList<String> producers; 
	public static String numConsumers;
	public static ArrayList<String> consumers;
	public static String starttime;
	
	public static ArrayList<String> statusFields;
	public static String numStatus;
	/*consumer properties */
	public static String zookeeper_server;

	private ElasticSingleton() {
	}

	public static Client getInstance() {
		String ret = null ;
		if (client == null) 
			ret = readINI();
		else 
			return client;
		
		if (!ret.equals(ERRORCONSTANTS.ES_INI_FILE_NOT_FOUND)) {
			
			try{
				client = TransportClient.builder().build()
				        .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("45.55.28.43"), 9300));
			} catch (Exception e){ 
				e.printStackTrace();
			}
		} 
		else 
			System.out.println(ERRORCONSTANTS.ES_INI_FILE_NOT_FOUND);
		return client;
	}

	private static String readINI() {
		
		File file = new File("src/main/resources/es.ini");
		try {
			if (!file.exists()) {
				return ERRORCONSTANTS.ES_INI_FILE_NOT_FOUND.toString();
			}
			Ini ini = new Ini(file);
			ES_IP = ini.get("ES_INFO", "ES_IP");
			indexName = ini.get("INDEX", "INDEX_NAME");
			nodeName = ini.get("ESNODE", "NODE");
			kafkaServer = ini.get("KAFKA_SERVER", "SERVER");
			starttime = ini.get("TIME", "START");
			numProducer = ini.get("PRODUCERS","NUM");
			
			producers = new ArrayList<String>(Integer.parseInt(numProducer));
			producers.add("input");
			
			numConsumers = ini.get("CONSUMERS","NUM");
			consumers = new ArrayList<String>(Integer.parseInt(numConsumers));
			for (int i = 1; i <= Integer.parseInt(numConsumers) ;i++)
				consumers.add(ini.get("CONSUMER_NAME", "ml_algo"+i));
			
			statusFields = new ArrayList<String>(Integer.parseInt(numConsumers));
			
			for (int i = 1; i <= Integer.parseInt(numConsumers) ;i++)
				statusFields.add(ini.get("CONSUMER_NAME", "ml_algo"+i+"_status"));
			
			zookeeper_server = ini.get("ZOOKEEPER_PROP","SERVER");
			
		} catch (Exception error) {
			System.out.println(error.getStackTrace());
		}
		return ES_IP;
	}
	
	public static void main(String args[]) throws IOException {
		ElasticSingleton s = new ElasticSingleton();
		ElasticSingleton.getInstance();
		ProducerWorker pq = new ProducerWorker();
		pq.start();
		ArrayList<UpdateES> listProducers = new ArrayList<UpdateES>();
		
		for (int i = 0; i < Integer.parseInt(numConsumers) ;i++)
		{	
			UpdateES p = new UpdateES(statusFields.get(i),consumers.get(i));
			listProducers.add(p);
			p.start();
			
		}
	}
}


