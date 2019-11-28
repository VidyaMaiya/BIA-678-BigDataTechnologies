package bia678project.kafka_yahoo_twitter;
import bia678project.kafka_yahoo_twitter.TwitterProducer;
import bia678project.kafka_yahoo_twitter.YahooStockQuoteAPI;
import bia678project.kafka_yahoo_twitter.KafkaConsumerThread;

public class MainStreaming {
	public static void main(String[] args) {
		Thread twitterThread = new Thread(new TwitterProducer());
		Thread yahooThread = new Thread(new YahooStockQuoteAPI()); 
		
		twitterThread.start();
		yahooThread.start();
		
		Thread kafkaTwitterConsumer = new Thread(new KafkaConsumerThread("twitter_tweets","console-consumer-twitterapp"));
		Thread kafkaYahooConsumer = new Thread(new KafkaConsumerThread("yahoo_stock_quote","console-consumer-yahooapp"));
		
		kafkaTwitterConsumer.start();
		kafkaYahooConsumer.start();
		
		try {
			twitterThread.join();
			yahooThread.join();
			kafkaTwitterConsumer.join();
			kafkaYahooConsumer.join();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
}
