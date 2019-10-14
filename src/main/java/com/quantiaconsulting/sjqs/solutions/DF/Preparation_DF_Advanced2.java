package com.quantiaconsulting.sjqs.solutions.DF;

import com.quantiaconsulting.sjqs.solutions.ML.BikeSharing;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;

import static org.apache.spark.sql.functions.*;

public class Preparation_DF_Advanced2 {
    public static void main(String[] args) {

        String path = BikeSharing.class.getProtectionDomain().getCodeSource().getLocation().getPath();
        String decodedPath = null;
        try {
            decodedPath = URLDecoder.decode(path.substring(0, path.lastIndexOf("/")), "UTF-8");
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        String parquetFile = decodedPath + "/resources/wikipedia_pageviews_by_second.parquet";
        
        SparkSession spark = SparkSession
                .builder()
                .appName("Simple Application")
                .getOrCreate();

        Dataset<Row> tempDF = spark
                .read()
                .parquet(parquetFile);

        tempDF.cache();
        tempDF.printSchema();

        //Group by and aggregation
        Dataset<Row> grouped = tempDF
                .withColumn("capturedAt", unix_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss"))
                .withColumn("year", year(col("timestamp")))
                .withColumn("month", month(col("timestamp")))
                .groupBy("year", "site")
                .sum("requests");

        //.agg(expr("sum(requests) as SUM")); if we want to give a name to the new column

        grouped.show(10);

        spark.stop();
    }
}
