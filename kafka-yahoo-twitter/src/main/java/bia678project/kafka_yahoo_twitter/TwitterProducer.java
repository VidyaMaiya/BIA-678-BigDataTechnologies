package bia678project.kafka_yahoo_twitter;


import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

/**
 * Hello world!
 *
 */
public class TwitterProducer {
	Logger logger = LoggerFactory.getLogger(TwitterProducer.class.getName());

	public TwitterProducer() {

	}

	public static void main(String[] args) {
		new TwitterProducer().run();
	}

	public void run() {
		logger.info("Setup");
		/**
		 * Set up your blocking queues: Be sure to size these properly based on expected
		 * TPS of your stream
		 * Client will put the message into msgQueue
		 */
		BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(1000);

		// Create a twitter client
		Client client = createTwitterClient(msgQueue);
		client.connect();

		// Create a kafka producer
		KafkaProducer<String, String> producer = createKafkaProducer();
		
		//add a shutdown hook
		Runtime.getRuntime().addShutdownHook(new Thread(() -> {
			logger.info("stopping application");
			logger.info("Shutting down twitter client");
			client.stop();
			logger.info("Closing Producer");
			producer.close();
			logger.info("Done!");
		}));

		// loop to send twitter to kafka
		while (!client.isDone()) {
			String msg = null;
			try {
				msg = msgQueue.poll(5, TimeUnit.SECONDS);
			} catch (InterruptedException e) {
				e.printStackTrace();
				client.stop();
			}
			if (msg != null) {
				//Send twitter feeds to kafka producer
				logger.info(msg);
				producer.send(new ProducerRecord<String, String>("twitter tweets", null, msg), new Callback() {

					@Override
					public void onCompletion(RecordMetadata metadata, Exception exception) {
						// TODO Auto-generated method stub
						if (exception != null) {
							logger.error("Something bad happened", exception);
						}
					}
				});
			}
		}
		logger.info("End of application");
	}

	String consumerKey = "9ITUSEAMB6Y78GjrvbPg4kfGf";
	String consumerSecret = "WW7aWlIdp1XKVgklPgVfMbJG1ROSG0QxyHgPc8N7SIqSdZmLrq";
	String token = "828212092666720257-1XdAnwQyEmI4jjO2TnDOqifN8s31iWg";
	String secret = "mwCm05P96lTF9uLiomS1cKJnFQbIuJ0GMdczCzAVABHbB";
	List<String> terms = Lists.newArrayList("stock price");

	public Client createTwitterClient(BlockingQueue<String> msgQueue) {

		/**
		 * Declare the host you want to connect to, the endpoint, and authentication
		 * (basic auth or oauth)
		 */
		Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
		StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
		// Optional: set up some followings and track terms

		hosebirdEndpoint.trackTerms(terms);

		// These secrets should be read from a config file
		Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, secret);

		//Craete client, which connects to STREAM_HOST, for authentication use hosebirdAuth
		ClientBuilder builder = new ClientBuilder().name("Hosebird-Client-01") // optional: mainly for the logs
				.hosts(hosebirdHosts).authentication(hosebirdAuth).endpoint(hosebirdEndpoint)
				.processor(new StringDelimitedProcessor(msgQueue)); // optional: use this if you want to process client
																	// events

		Client hosebirdClient = builder.build();
		// Attempts to establish a connection.
		return hosebirdClient;
	}

	public KafkaProducer<String, String> createKafkaProducer() {
		String bootstrapServers = "127.0.0.1:9092";
		// Create Producer Properties
		Properties properties = new Properties();
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

		// Create Producer
		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
		return producer;
	}
}

