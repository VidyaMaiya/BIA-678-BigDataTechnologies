package bia678project.kafka_yahoo_twitter;
import bia678project.kafka_yahoo_twitter.TwitterProducer;
import bia678project.kafka_yahoo_twitter.YahooStockQuoteAPI;
import bia678project.kafka_yahoo_twitter.YahooStockQuote;
import bia678project.kafka_yahoo_twitter.KafkaConsumerThread;

public class MainStreaming {
	public static void main(String[] args) {
		Thread twitterThread = new Thread(new TwitterProducer());
		//Thread yahooThread = new Thread(new YahooStockQuoteAPI()); 
		YahooStockQuote yahooStock = new YahooStockQuote();
		Thread yahooStockProducer = new Thread(new Runnable() {
			@Override
			public void run() {
				try {
					yahooStock.produce();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		});
		Thread yahooStockConsumer = new Thread(new Runnable() {
			@Override
			public void run() {
				try {
					yahooStock.consume();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		});
		
		twitterThread.start();
		//yahooThread.start();
		yahooStockProducer.start();
		yahooStockConsumer.start();
		
		Thread kafkaTwitterConsumer = new Thread(new KafkaConsumerThread("twitter_tweets","console-consumer-twitterapp"));
		Thread kafkaYahooConsumer = new Thread(new KafkaConsumerThread("yahoo_stock_quote","console-consumer-yahooapp"));
		
		kafkaTwitterConsumer.start();
		kafkaYahooConsumer.start();
		
		try {
			twitterThread.join();
			//yahooThread.join();
			yahooStockProducer.join();
			yahooStockConsumer.join();
			kafkaTwitterConsumer.join();
			kafkaYahooConsumer.join();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
}
