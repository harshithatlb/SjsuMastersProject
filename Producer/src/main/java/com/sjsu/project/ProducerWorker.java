package com.sjsu.project;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;

import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

public class ProducerWorker extends Thread {
	
	public Properties properties;
	String time;
	DateTime dt_time;
	
	ProducerWorker(){
		properties = new Properties();
	    properties.put("serializer.class", "kafka.serializer.StringEncoder");
	    properties.put("request.required.acks", "1");
	    properties.put("bootstrap.servers", ElasticSingleton.kafkaServer);
	    properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
	    properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
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
	 * This function gets the Query Results based on RangeQuery on timestamp
	 * 
	 */
	void getQueryResults() throws  IOException {
		
		DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSS");
		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
		DateTime dt_cur;
		
		QueryBuilder qb = null;
		if (dt_time == null)
			 qb = QueryBuilders.boolQuery().must(QueryBuilders.rangeQuery("@timestamp").gt(ElasticSingleton.starttime));
		else
			 qb = QueryBuilders.boolQuery().must(QueryBuilders.rangeQuery("@timestamp").gt(dt_time.toString()));
			
		
		SearchRequestBuilder resp = ElasticSingleton.client.prepareSearch(ElasticSingleton.indexName).setQuery(qb);
		
		// Executing the search and stores the result in response with a size of 100 
		// scroll parameter : Elastic search how long it should keep the search context alive
		// TimeValue is in milliseconds = 60 seconds
		SearchResponse response = resp.setScroll(new TimeValue(100)).setSize(100).execute().actionGet();
		// while loops till the response is ended, each response size has to be completed within scroll
		while (true) {
			
		    for (SearchHit hit : response.getHits().getHits()) {
		    	
		    	Map<String, Object> res = hit.getSource();
		    	System.out.println("hit: " + hit.getSourceAsString());
		    	String str = hit.getId()+"|"+res.get("pkts").toString() + ","+res.get("bytes") + "," +
		    			res.get("flow_duration") +  "," + process(res.get("flow_closure_flag").toString());
		    	System.out.println("ddos: " + res.get("ml_ddos_status"));
		    	
		    	//	System.out.println( res.toString() + "ddos" +res.get("ml_ddos_status"));
		    
				if (dt_time == null)
					dt_time = formatter.parseDateTime(res.get("@timestamp").toString());
				else{
			    	dt_cur = formatter.parseDateTime(res.get("@timestamp").toString());
			    	
			    	if (dt_cur.compareTo(dt_time) > 0)
			    		dt_time = dt_cur;
		    	}
		    	for (String qname : ElasticSingleton.producers)
		    		producer.send(new ProducerRecord<String, String>(qname, str));
		    	System.out.println(str);
		    	
		    }
		    response = ElasticSingleton.client.prepareSearchScroll(response.getScrollId()).setScroll(new TimeValue(100)).execute().actionGet();
		    
		    if (response.getHits().getHits().length == 0) {
		        break;
		    }
		}
		producer.close();
	}

	public void run() {
		
		while(true)
		{	
            try {   	
            	getQueryResults();
                Thread.sleep(6000);
                
            } catch (InterruptedException e) {
                break;
            } catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
        }       
	}

}
