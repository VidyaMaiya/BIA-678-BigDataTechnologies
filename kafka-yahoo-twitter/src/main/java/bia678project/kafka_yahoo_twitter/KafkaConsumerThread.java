package bia678project.kafka_yahoo_twitter;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaConsumerThread implements Runnable {
	Logger logger = LoggerFactory.getLogger(KafkaConsumerThread.class.getName());
	private String topicName;
	private String groupId;

	KafkaConsumerThread(String topicName,String groupId) {
		this.topicName = topicName;
		this.groupId = groupId;
	}

	public void run() {
		KafkaConsumer<String, String> consumer = createKafkaConsumer();
		consumer.subscribe(Arrays.asList(topicName));
		
		// add a shutdown hook
		Runtime.getRuntime().addShutdownHook(new Thread(() -> {
			logger.info("stopping application");
			logger.info("Closing Consumer");
			consumer.close();
			logger.info("Done!");
		}));
		
		while (true) {
			/*
			 * Consumer polls the Kafka server every 1 sec to check if there are any new
			 * messages in the topic
			 */
			ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
			for (ConsumerRecord<String, String> record : records) {
				System.out.println(record.value());
			}
		}
	}

	public KafkaConsumer<String, String> createKafkaConsumer() {
		//String bootstrapServers = "127.0.0.1:9092";
		String bootstrapServers = "localhost:9092";
		
		// Create Consumer Properties
		Properties properties = new Properties();
		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
		properties.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, "simple");

		// Create Consumer
		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
		return consumer;
	}

}
