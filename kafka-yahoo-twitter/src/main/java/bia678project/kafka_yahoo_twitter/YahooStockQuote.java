package bia678project.kafka_yahoo_twitter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
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

import yahoofinance.Stock;
import yahoofinance.YahooFinance;
import yahoofinance.quotes.stock.StockQuote;

public class YahooStockQuote {
	Logger logger = LoggerFactory.getLogger(TwitterProducer.class.getName());

	YahooStockQuote() {
		System.out.println("Starting Yahoo Quote API");
	}

	int capacity = 1000;
	BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(capacity);

	public void produce() throws InterruptedException {
		logger.info("Start fetching from YahooStockQuote API");
		String[] symbols = new String[] { "INTC", "BABA", "TSLA", "AIR.PA", "GOOG", "MSFT" };
		Map<String, Stock> stocks = null;
		while (true) {
			try {
				stocks = YahooFinance.get(symbols);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			for (String symbol : symbols) {
				synchronized (this) {
					while (msgQueue.size() == capacity) {
						wait();
					}
					Stock companyStock = stocks.get(symbol);
					StockQuote companySq = companyStock.getQuote();
					String intcQuote = "{Symbol: " + companySq.getSymbol() + ", Price:" + companySq.getPrice()
							+ ", Date: " + companySq.getLastTradeTime().getTime() + "} \n";
					msgQueue.add(intcQuote);
					notify();
					Thread.sleep(1000);
				}

			}
		}
	}

	public void consume() throws InterruptedException {
		logger.info("Start consuming YahooStockQuote API");
		KafkaProducer<String, String> producer = createKafkaProducer();

		Runtime.getRuntime().addShutdownHook(new Thread(() -> {
			logger.info("stopping yahoo api application");
			logger.info("Closing yahoo api Producer");
			producer.close();
			logger.info("Done closing yahoo api!");
		}));

		String msg = null;
		while (true) {
			synchronized (this) {
				while (msgQueue.size() == 0) {
					wait();
				}
				try {
					msg = msgQueue.poll(5, TimeUnit.SECONDS);
					notify();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				if (msg != null) {
					logger.info(msg);
					producer.send(new ProducerRecord<String, String>("yahoo_stock_quote", null, msg), new Callback() {
						@Override
						public void onCompletion(RecordMetadata metadata, Exception exception) {
							// TODO Auto-generated method stub
							if (exception != null) {
								logger.error("Something bad happened", exception);
							}
						}
					});
				}
				Thread.sleep(1000);
			}
		}
	}

	public KafkaProducer<String, String> createKafkaProducer() {
		// String bootstrapServers = "127.0.0.1:9092";
		String bootstrapServers = "localhost:9092";

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
