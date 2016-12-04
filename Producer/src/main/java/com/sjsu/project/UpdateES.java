
package com.sjsu.project;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;


public class UpdateES extends Thread{
	KafkaConsumer<String, String> consumer;
	Properties props;
	private String topic;
	
	UpdateES(String grp, String topic){
		props = new Properties();
        props.put("zookeeper.connect", ElasticSingleton.zookeeper_server);
        props.put("group.id", grp);
        props.put("zookeeper.session.timeout.ms", "400");
        props.put("zookeeper.sync.time.ms", "200");
        props.put("auto.commit.interval.ms", "1000");
        props.put("bootstrap.servers", ElasticSingleton.kafkaServer);
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumer = new KafkaConsumer
                <String, String>(props);
        this.topic = topic;
	}
	
	
	public void run() {
        
		consumer.subscribe(Collections.singletonList(topic));
		
        try{
        	while (true) {
	            ConsumerRecords<String, String> records = consumer.poll(Long.MAX_VALUE);
	            for (ConsumerRecord<String, String> record : records) {
	            	System.out.println(record.value());
	            	updateRecord_StateInES(record.value(), "1");
	            }
        	}
        }catch(WakeupException e ) {
        	//ignore
        } catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ExecutionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally{
			consumer.close();
		}
        	
	}
        
	public void shutdown() {
		System.out.println("waking up");
	    consumer.wakeup();
	  }
	
	private static void updateRecord_StateInES(String recordId, String state) throws IOException, InterruptedException, ExecutionException{
		ElasticSingleton.getInstance();
		XContentBuilder jb_status;
		IndexRequest indexRequest;
		UpdateRequest updateRequest_status;
		jb_status = XContentFactory.jsonBuilder();

		jb_status.startObject();
		jb_status.field("ml_ddos_status",state);
		jb_status.endObject();

		indexRequest = new IndexRequest(ElasticSingleton.indexName, ElasticSingleton.indexName,recordId)
		        .source(jb_status);
		
		updateRequest_status = new UpdateRequest(ElasticSingleton.indexName, ElasticSingleton.indexName,recordId)
		        .doc(jb_status)
		        .upsert(indexRequest);              
		//System.out.println(updateRequest_status);
		ElasticSingleton.client.update(updateRequest_status).get();
		return;
	}
}
