package com.johnmedlock.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.logging.Level;
import java.util.logging.Logger;

public class LogStreamAnalysis {

    public static void main(String[] args) throws InterruptedException {
        Logger.getLogger("org.apache").setLevel(Level.WARNING);
        Logger.getLogger("org.apache.spark.storage").setLevel(Level.SEVERE);

//        boilerplate
        SparkConf conf = new SparkConf()
                .setMaster("local[*]")
                .setAppName("StreamingSpark");

//        Streaming config and duration of update
        JavaStreamingContext sc = new JavaStreamingContext(conf, Durations.seconds(2));

//        Source of data server
        JavaReceiverInputDStream<String> inputData = sc
                .socketTextStream("localhost", 8989);

        JavaDStream<Object> results = inputData
                .map(v1 -> v1);

//        maps values to tuple2s with the string values and 1L
        JavaPairDStream<String, Long> pairDstream = results
                .mapToPair(o -> new Tuple2<>(o.toString().split(",")[0], 1L));

//        reduces the tuples (x,1) -> (x,sum(1))
//        pairDstream = pairDstream.reduceByKey((v1, v2) -> v1 + v2);
        pairDstream = pairDstream.reduceByKeyAndWindow((v1, v2) -> v1 + v2, Durations.minutes(2));


        //prints top ten
        pairDstream.print();

        sc.start();
        sc.awaitTermination();
    }
}
