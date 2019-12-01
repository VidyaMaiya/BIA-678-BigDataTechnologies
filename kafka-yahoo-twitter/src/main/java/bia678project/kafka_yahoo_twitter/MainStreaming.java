package bia678project.kafka_yahoo_twitter;

public class MainStreaming {
    public static final String KAFKA_BROKER = "localhost:9092";
    public static final long pullingDuration = 1L;

    public static void main(String[] args) {
        DataStreamingService dss = DataStreamingService.getSparkContext(KAFKA_BROKER, pullingDuration);
        Thread twitterThread = new Thread(new TwitterProducer());
        Thread yahooThread = new Thread(new YahooStockQuoteAPI());

        twitterThread.start();
        yahooThread.start();

		/* TODO - delete this comment
		Thread kafkaTwitterConsumer = new Thread(new KafkaConsumerThread("twitter_tweets","console-consumer-twitterapp"));
		Thread kafkaYahooConsumer = new Thread(new KafkaConsumerThread("yahoo_stock_quote","console-consumer-yahooapp"));

		kafkaTwitterConsumer.start();
		kafkaYahooConsumer.start();
		*/
        try {
            dss.connectToKafka();
            dss.startConsuming();

		/*	TODO - remove comments
			twitterThread.join();
			yahooThread.join();
			kafkaTwitterConsumer.join();
			kafkaYahooConsumer.join();
			*/
        } catch (InterruptedException e) {

            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }
}
