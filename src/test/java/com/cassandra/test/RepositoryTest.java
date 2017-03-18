package com.cassandra.test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.listener.config.ContainerProperties;
import org.springframework.kafka.support.TopicPartitionInitialOffset;
import org.springframework.util.concurrent.ListenableFutureCallback;




public class RepositoryTest extends BaseTest {
	private static final String TEMPLATE_TOPIC = "templateTopic";
	
	@Autowired
	ApplicationContext applicationContext;
	
	@Autowired
	private KafkaTemplate<Integer, String> kafkaTemplate;
	
	@Test
	public void orderTest() {
		
		ContainerProperties containerProperties = new ContainerProperties(TEMPLATE_TOPIC);
		
		DefaultKafkaConsumerFactory<Integer, String> cf=(DefaultKafkaConsumerFactory<Integer, String>) applicationContext.getBean("defaultKafkaConsumerFactory");
		
		TopicPartitionInitialOffset topicPartition1=new TopicPartitionInitialOffset(TEMPLATE_TOPIC,1);
		
		KafkaMessageListenerContainer<Integer, String> container1 = new KafkaMessageListenerContainer<>(cf,
				containerProperties,topicPartition1);
		
		final BlockingQueue<ConsumerRecord<Integer, String>> records = new LinkedBlockingQueue<>();
		container1.setupMessageListener(new MessageListener<Integer, String>() {
			@Override
			public void onMessage(ConsumerRecord<Integer, String> record) {
				System.out.println(record);
				records.add(record);
			}
		});
		container1.setBeanName("templateTests");
		
		try {
			
			
			 kafkaTemplate.send("evaluateTopic",1, "wahaha").addCallback(new ListenableFutureCallback<Object>() {
					@Override
					public void onFailure(Throwable e) {
						System.out.println("发送失败.");
						e.printStackTrace();
					
					}

					@Override 
					public void onSuccess(Object result) {
						System.out.println("发送成功.");
					}
				});
		} catch (Exception e1) {
			System.out.println("发送异常");
			// TODO: handle exception
			e1.printStackTrace();
		}
		
		
		
		container1.start();
		
		
		
		try {
			
			
			 kafkaTemplate.send("evaluateTopic",1, "wahaha11").addCallback(new ListenableFutureCallback<Object>() {
					@Override
					public void onFailure(Throwable e) {
						System.out.println("发送失败.");
						e.printStackTrace();
					
					}

					@Override 
					public void onSuccess(Object result) {
						System.out.println("发送成功.");
					}
				});
		} catch (Exception e1) {
			System.out.println("发送异常");
			// TODO: handle exception
			e1.printStackTrace();
		}
		
		try {
			
			
			 kafkaTemplate.send("evaluateTopic",0, "wahaha12").addCallback(new ListenableFutureCallback<Object>() {
					@Override
					public void onFailure(Throwable e) {
						System.out.println("发送失败.");
						e.printStackTrace();
					
					}

					@Override 
					public void onSuccess(Object result) {
						System.out.println("发送成功.");
					}
				});
		} catch (Exception e1) {
			System.out.println("发送异常");
			// TODO: handle exception
			e1.printStackTrace();
		}
		
		
		
		
		container1.stop();
		
		try {
			
			
			 kafkaTemplate.send("evaluateTopic",1, "wahaha3").addCallback(new ListenableFutureCallback<Object>() {
					@Override
					public void onFailure(Throwable e) {
						System.out.println("发送失败.");
						e.printStackTrace();
					
					}

					@Override 
					public void onSuccess(Object result) {
						System.out.println("发送成功.");
					}
				});
		} catch (Exception e1) {
			System.out.println("发送异常");
			// TODO: handle exception
			e1.printStackTrace();
		}

	
		
	}


}
