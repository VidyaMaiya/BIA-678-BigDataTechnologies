package bia678project.kafka_yahoo_twitter;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;


import java.util.*;

public class DataStreamingService {
    private SparkConf sparkConf;
    private String appName;
    private String kafkaBroker;
    private String groupId;
    private List<String> topics;
    private static DataStreamingService instance = null;
    private JavaStreamingContext streamingContext;
    private long pullTimeFreq;

    private DataStreamingService(String kafkaBroker, long pullTimeFreq){
        this.appName = "dataStreamingService";
        this.sparkConf = new SparkConf()
                .setAppName(appName)
                .setMaster("local[5]");

        this.kafkaBroker = kafkaBroker;
        this.topics = new ArrayList<>();
        this.topics.add("twitter_tweets");
        this.topics.add("yahoo_stock_quote");
        this.pullTimeFreq = pullTimeFreq;

    }

    public static DataStreamingService getSparkContext(String kafkaBroker, long pullTimeFreq){
        if(instance != null) return instance;
        return new DataStreamingService(kafkaBroker, pullTimeFreq);
    }
    public void connectToKafka(){
        System.out.println("Trying to connect to kafka");
        this.streamingContext = new JavaStreamingContext(this.sparkConf, new Duration(1000L));
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", kafkaBroker);
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "console-consumer-twitterapp");
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", false);
        Collection<String> topics = this.topics;


        readMessages(kafkaParams, topics);

    }

    private void readMessages(Map<String, Object> kafkaParams, Collection<String> topics) {
        JavaInputDStream<ConsumerRecord<String, String>> messages =
                KafkaUtils.createDirectStream(
                        streamingContext,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.<String, String> Subscribe(Arrays.asList("twitter_tweets"), kafkaParams));

        System.out.println("Spark and kafka connected!!");
        messages.foreachRDD((k,v) -> {
            k.foreach((a) -> System.out.println(a.value()));

        });

        System.out.println("Spark and kafka connected!!");

    }

    public void startConsuming() throws InterruptedException {
        streamingContext.start();
        streamingContext.awaitTermination();

    }
}
