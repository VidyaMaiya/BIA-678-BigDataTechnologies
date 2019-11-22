package bia678project.kafka_yahoo_twitter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

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

public class YahooStockQuoteAPI {
	Logger logger = LoggerFactory.getLogger(TwitterProducer.class.getName());

	YahooStockQuoteAPI() {
	}

	public static void main(String[] args) {
		new YahooStockQuoteAPI().run();
	}

	public void run() {
		List<List<String>> yahooStockQuote = new YahooStockQuoteAPI().getYahooStockQuote();

		// Create a kafka producer
		KafkaProducer<String, List<String>> producer = createKafkaProducer();

		for (List<String> stock : yahooStockQuote) {
			if (!stock.isEmpty()) {
				logger.info(stock.toString());
				producer.send(new ProducerRecord<String, List<String>>("yahoo_stock_quote", null, stock),
						new Callback() {

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

	public List<List<String>> getYahooStockQuote() {
		List<List<String>> yahooStockQuote = new ArrayList<List<String>>();
		List<String> companyStockQuote = new ArrayList<String>();

		String[] symbols = new String[] { "INTC", "BABA", "TSLA", "AIR.PA", "GOOG", "MSFT" };
		Map<String, Stock> stocks = null;
		try {
			stocks = YahooFinance.get(symbols);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		Stock intc = stocks.get("INTC");
		StockQuote intcSq = intc.getQuote();
		String intcQuote = "{Symbol: " + intcSq.getSymbol() + ", Price:" + intcSq.getPrice() + ", Date: "
				+ intcSq.getLastTradeTime().getTime() + "} \n";
		companyStockQuote.add(intcQuote);

		Stock baba = stocks.get("BABA");
		StockQuote babaSq = baba.getQuote();
		String babaQuote = "{Symbol: " + babaSq.getSymbol() + ", Price:" + babaSq.getPrice() + ", Date: "
				+ babaSq.getLastTradeTime().getTime() + "}\n";
		companyStockQuote.add(babaQuote);

		Stock tsla = stocks.get("TSLA");
		StockQuote tslaSq = tsla.getQuote();
		String tslaQuote = "{Symbol: " + tslaSq.getSymbol() + ", Price:" + tslaSq.getPrice() + ", Date: "
				+ tslaSq.getLastTradeTime().getTime() + "}\n";
		companyStockQuote.add(tslaQuote);

		Stock airpa = stocks.get("AIR.PA");
		StockQuote airpaSq = airpa.getQuote();
		String airpaQuote = "{Symbol: " + airpaSq.getSymbol() + ", Price:" + airpaSq.getPrice() + ", Date: "
				+ airpaSq.getLastTradeTime().getTime() + "}\n";
		companyStockQuote.add(airpaQuote);

		Stock google = stocks.get("GOOG");
		StockQuote googleSq = google.getQuote();
		String googleQuote = "{Symbol: " + googleSq.getSymbol() + ", Price:" + googleSq.getPrice() + ", Date: "
				+ googleSq.getLastTradeTime().getTime() + "}\n";
		companyStockQuote.add(googleQuote);

		Stock msft = stocks.get("MSFT");
		StockQuote msftSq = msft.getQuote();
		String msftQuote = "{Symbol: " + msftSq.getSymbol() + ", Price:" + msftSq.getPrice() + ", Date: "
				+ msftSq.getLastTradeTime().getTime() + "}\n";
		companyStockQuote.add(msftQuote);

		yahooStockQuote.add(companyStockQuote);

		return yahooStockQuote;
	}

	public KafkaProducer<String, List<String>> createKafkaProducer() {
		String bootstrapServers = "127.0.0.1:9092";

		// Create Producer Properties
		Properties properties = new Properties();
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

		// Create Producer
		KafkaProducer<String, List<String>> producer = new KafkaProducer<String, List<String>>(properties);
		return producer;
	}
}
