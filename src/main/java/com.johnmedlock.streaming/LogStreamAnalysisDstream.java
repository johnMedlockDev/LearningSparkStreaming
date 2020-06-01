package com.johnmedlock.streaming;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

public class LogStreamAnalysisDstream {

    public static void main(String[] args) throws InterruptedException {
        Logger.getLogger("org.apache").setLevel(Level.WARNING);
        Logger.getLogger("org.apache.spark.storage").setLevel(Level.SEVERE);

//        boilerplate
        SparkConf conf = new SparkConf()
                .setMaster("local[*]")
                .setAppName("StreamingSpark");

//        Streaming config and duration of update
        JavaStreamingContext sc = new JavaStreamingContext(conf, Durations.seconds(1));

        Collection<String> topics = Arrays.asList("viewrecords");

        Map<String, Object> params = new HashMap<>();
        params.put("bootstrap.servers", "localhost:9092");
        params.put("key.deserializer", StringDeserializer.class);
        params.put("value.deserializer", StringDeserializer.class);
        params.put("group.id", "spark-group");
        params.put("auto.offset.reset", "latest");
        params.put("enable.auto.commit", false);

        JavaInputDStream<ConsumerRecord<String, String>> stream = KafkaUtils
                .createDirectStream(
                        sc,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.<String, String>Subscribe(topics, params)
                );

        JavaPairDStream<Long, String> results = stream
                .mapToPair(stringStringConsumerRecord -> new Tuple2<>(stringStringConsumerRecord.value(), 5L))
                .reduceByKeyAndWindow((v1, v2) -> v1 + v2, Durations.minutes(60))
                .mapToPair(stringLongTuple2 -> stringLongTuple2.swap())
                .transformToPair((v1, v2) -> v1.sortByKey(false));

        results.print();

        sc.start();
        sc.awaitTermination();
    }
}
