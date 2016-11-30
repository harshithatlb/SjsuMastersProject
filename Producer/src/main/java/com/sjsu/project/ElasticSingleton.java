package com.sjsu.project;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Properties;

import org.elasticsearch.index.query.TermQueryBuilder;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
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
public class ElasticSingleton {

	public static Client client;
	public static String ES_IP;
	public static String prod_queue;
	public static Properties properties;
	public static String indexName;
	public static String nodeName;
	public static String kafkaServer;
	
	private ElasticSingleton() {
	}

	public static Client getInstance() {
		if (client == null) {
			ES_IP = readINI();
		} else {
			return client;
		}
		if (!ES_IP.equals(ERRORCONSTANTS.ES_INI_FILE_NOT_FOUND)) {
			try {
				client = TransportClient.builder().build()
				        .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(nodeName), 9300));
			} catch (Exception e) {
				e.printStackTrace();
			}
		} else {
			System.out.println(ERRORCONSTANTS.ES_INI_FILE_NOT_FOUND);
		}
		properties = new Properties();
        properties.put("serializer.class", "kafka.serializer.StringEncoder");
        properties.put("request.required.acks", "1");
        properties.put("bootstrap.servers", kafkaServer);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		return client;
	}

	private static String readINI() {
		String ES_IP = null;

		File file = new File("es.ini");

		try {

			if (!file.exists()) {
				return ERRORCONSTANTS.ES_INI_FILE_NOT_FOUND.toString();
			}
			Ini ini = new Ini(file);
			ES_IP = ini.get("ES_INFO", "ES_IP");
			prod_queue=ini.get("KAFKA", "PROD_Q");
			kafkaServer=ini.get("KAFKA", "SERVER");
			nodeName = ini.get("ES_INFO", "NODE");
			indexName = ini.get("INDEX","INDEX_NAME");
			
		} catch (Exception error) {

		}
		return ES_IP;
	}

	public static void main(String args[]) throws IOException {
		ElasticSingleton s = new ElasticSingleton();
		ElasticSingleton.getInstance();
		s.runQuery();
	}
	String process(String  flow_closure_flag ){
		int A_flag = 0 ,F_flag = 0,P_flag= 0,R_flag = 0,S_flag =0 ;
		if ( flow_closure_flag.contains("HALF_CLOSE_TO"))
		{   //SAPF
			
			F_flag = 1;
			A_flag = 1;
			S_flag = 1;
			P_flag = 1;
		}
		else if ( flow_closure_flag.contains("FULL_CLOSE"))
		{
			// SAF or SAPR
			R_flag = 1;
			A_flag = 1;
			S_flag = 1;
			F_flag = 1;
			P_flag = 1;
		}
		else if ( flow_closure_flag.contains("SYN_ONLY_TO"))
		{	// S or SAP
			S_flag = 1;
			A_flag = 1;
			P_flag = 1;
		}
		else if ( flow_closure_flag.contains("RST"))
		{   //SAPR
			R_flag = 1;
			A_flag = 1;
			S_flag = 1;
			P_flag = 1;
		}
		else if ( flow_closure_flag.contains("Other_TO"))
		{	// could be SAP
			A_flag = 1;
			S_flag = 1;
			P_flag = 1;
		}
		
		return   A_flag +"," + F_flag + "," +P_flag +"," +R_flag +","+ S_flag;
	}

	/*
	Example Reference
	BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
		boolQueryBuilder.must(QueryBuilders.rangeQuery("@timestamp").gt("now-6M/d"));
		
	Query to get the index of a record 
	TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery("_id", "AVXnmkRkD8hgobG71RJv");
	*/
	
	/*
	 * This function gets the Query Results based on RangeQuery on timestamp
	 * 
	 */
	void getQueryResults(String timeval) throws  IOException {
		
		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
		
		QueryBuilder qb = null;
		if (timeval == null)
			 qb = QueryBuilders.boolQuery().must(QueryBuilders.rangeQuery("@timestamp").gt("now-40d"));
		else{
			 qb = QueryBuilders.boolQuery().must(QueryBuilders.rangeQuery("@timestamp").gt("now-40d"));
			 // we should do something here based on the parameter timeval
		}
		//creating the query
		@SuppressWarnings("deprecation")
		QueryBuilder query = (QueryBuilder) QueryBuilders.filteredQuery(null, qb );

		//Setting the query with an index
		
		SearchRequestBuilder resp = client.prepareSearch(indexName).setQuery(qb);
		
		// Executing the search and stores the result in response with a size of 100 
		// scroll parameter : Elastic search how long it should keep the search context alive
		// TimeValue is in milliseconds = 60 seconds
		SearchResponse response = resp.setScroll(new TimeValue(5)).setSize(100).execute().actionGet();
		// while loops till the response is ended, each response size has to be completed within scroll
		while (true) {
			
		    for (SearchHit hit : response.getHits().getHits()) {
		    	
		    	Map<String, Object> res = hit.getSource();
		    	String str = hit.getId()+"|"+res.get("pkts").toString() + res.get("bytes") + 
		    			res.get("flow_duration") + process(res.get("flow_closure_flag").toString());
		    	
				producer.send(new ProducerRecord<String, String>(prod_queue, str));
			}
		    response = client.prepareSearchScroll(response.getScrollId()).setScroll(new TimeValue(5)).execute().actionGet();
		    if (response.getHits().getHits().length == 0) {
		        break;
		    }
		}
	}
	void runQuery() throws IOException
	{	// Currently gives the same data on every run, 
		// need to discuss 
		int n = 3;
		while (n > 0)
		{	
			System.out.println("calling query");
			n--;
			// Everytime this is run, the code is hard coded to start with now-40d
			getQueryResults(null);
		}
	}

}
