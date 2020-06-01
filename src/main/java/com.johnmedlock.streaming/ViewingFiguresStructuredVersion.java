package com.johnmedlock.streaming;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

import java.util.logging.Level;
import java.util.logging.Logger;

public class ViewingFiguresStructuredVersion {

    public static void main(String[] args) throws StreamingQueryException {
        Logger.getLogger("org.apache").setLevel(Level.WARNING);
        Logger.getLogger("org.apache.spark.storage").setLevel(Level.SEVERE);

        SparkSession session = SparkSession
                .builder()
                .master("local[*]")
                .appName("Structured View")
                .getOrCreate();

        Dataset<Row> df = session
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("subscribe", "viewrecords")
                .load();

        df.createOrReplaceTempView("viewing_figures");


        Dataset<Row> results = session.sql("select value from viewing_figures");

       StreamingQuery query = results
                .writeStream()
                .format("console")
                .outputMode(OutputMode.Append())
                .start();

        query.awaitTermination();

    }

}
