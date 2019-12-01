package bia678project.kafka_yahoo_twitter;

public class MainStreaming {
    public static final String KAFKA_BROKER = "localhost:9092";
    public static final long pullingDuration = 1L;

    public static void main(String[] args) {
        DataStreamingService dss = DataStreamingService.getSparkContext(KAFKA_BROKER, pullingDuration);
        
        Thread twitterThread = new Thread(new TwitterProducer());
        twitterThread.start();
        
        YahooStockQuote yahooStock = new YahooStockQuote();
        Thread yahooStockProducer = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                	//Fetches data from YahooFinance API and writes them to Message Queue
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
					//Reads data from Message Queue, then Kafaka producer writes them to Kafka Topic
					yahooStock.consume();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		});
        yahooStockProducer.start();
        yahooStockConsumer.start();


        try {
            dss.connectToKafka();
            dss.startConsuming();

		/*	TODO - remove comments
			twitterThread.join();
			//yahooThread.join();
			yahooStockProducer.join();
			yahooStockConsumer.join();
			kafkaTwitterConsumer.join();
			kafkaYahooConsumer.join();
			*/
        } catch (InterruptedException e) {

            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }
}
