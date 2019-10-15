package com.quantiaconsulting.sjqs.df.codeBrowsing;

import com.quantiaconsulting.sjqs.solutions.ml.BikeSharing;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;

public class Ingestion_CSV_InferSchema {
    public static void main(String[] args) {
        String path = BikeSharing.class.getProtectionDomain().getCodeSource().getLocation().getPath();
        String decodedPath = null;
        try {
            decodedPath = URLDecoder.decode(path.substring(0, path.lastIndexOf("/")), "UTF-8");
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        String csvFile = decodedPath + "/resources/wikipedia_pageviews_by_second.csv";

        SparkSession spark = SparkSession
                .builder()
                .appName("Simple Application")
                .getOrCreate();

        Dataset<Row> tempDF = spark
                .read()
                .option("sep", ",")
                .option("header", "true")
                .option("inferSchema", "true")
                .csv(csvFile);

        tempDF.cache();
        tempDF.printSchema();
        tempDF.show(10);
        System.out.println(tempDF.count());

        spark.stop();
    }

}
