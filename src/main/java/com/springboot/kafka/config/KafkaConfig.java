package com.springboot.kafka.config;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import javax.annotation.PostConstruct;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.listener.config.ContainerProperties;
import org.springframework.kafka.support.TopicPartitionInitialOffset;

@Configuration
@EnableKafka
public class KafkaConfig {

	 KafkaProperties properties;



	private static final Logger log = LoggerFactory.getLogger(KafkaConfig.class);

	@Bean
	public Map<String, Object> consumerConfigs() {
		Map<String, Object> props = new HashMap<>();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, properties.getBrokers());
		props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-1");
		props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
		props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "15000");
		props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "50000");
		props.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, "60000");
		props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, "50000");
		props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG,200);
		props.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG,10485760);
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
				"org.apache.kafka.common.serialization.IntegerDeserializer");
		
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");
		return props;
	}
	
	@Bean
	public KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<Integer, String>> kafkaListenerContainerFactory() {
		  ConcurrentKafkaListenerContainerFactory<Integer, String> factory =
                  new ConcurrentKafkaListenerContainerFactory<>();
		factory.setConsumerFactory(ConsumerFactory());
		return factory;
	}
	@SuppressWarnings("rawtypes")
	@Bean
	public DefaultKafkaConsumerFactory defaultKafkaConsumerFactory() {
		DefaultKafkaConsumerFactory<Integer, String> cf = new DefaultKafkaConsumerFactory<Integer, String>(
				consumerConfigs());
		return cf;
	}
	
	
	@Bean
	public ConsumerFactory<Integer, String> ConsumerFactory() {
		Map<String, Object> configs = consumerConfigs();
		return new DefaultKafkaConsumerFactory<>(configs);
	}
	
	
	@Bean
	public KafkaTemplate<Integer, String> kafkaTemplate() {
		return new KafkaTemplate<Integer, String>(producerFactory());
	}

	@Bean
	public ProducerFactory<Integer, String> producerFactory() {
		return new DefaultKafkaProducerFactory<>(producerConfigs());
	}
	
	@Bean
	public Map<String, Object> producerConfigs() {
		Map<String, Object> props = new HashMap<>();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, properties.getBrokers());
		props.put(ProducerConfig.RETRIES_CONFIG, 3 );
		props.put(ProducerConfig.ACKS_CONFIG, "all");

		
		props.put(ProducerConfig.BATCH_SIZE_CONFIG, 10);
		props.put(ProducerConfig.SEND_BUFFER_CONFIG, 100000000);
		props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 60000);
		
		
		props.put(ProducerConfig.LINGER_MS_CONFIG, 5);
		props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 6000);
		//props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, properties.getBufferSize());
		props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 1000000000);
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,properties.getKeySerializer());
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, properties.getValueSerializer());
		return props;
	}
    @PostConstruct
    public void start() {
    	
    	
       ContainerProperties containerProperties = new ContainerProperties("templateTopic");
	   TopicPartitionInitialOffset topicPartition1=new TopicPartitionInitialOffset("templateTopic",1);
	   KafkaMessageListenerContainer<Integer, String> container1 = new KafkaMessageListenerContainer<>(defaultKafkaConsumerFactory(),
				containerProperties,topicPartition1);
		
		final BlockingQueue<ConsumerRecord<Integer, String>> records = new LinkedBlockingQueue<>();
		
		container1.setupMessageListener(new MessageListener<Integer, String>() {
			@Override
			public void onMessage(ConsumerRecord<Integer, String> record) {
				log.info(record.offset()+"---"+record.value());
				records.add(record);
			}
		});
		container1.setBeanName("templateTests");
		
		container1.start();
		

		
		
    }

}
