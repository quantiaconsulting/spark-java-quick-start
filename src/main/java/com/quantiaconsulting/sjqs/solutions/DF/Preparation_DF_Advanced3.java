package com.quantiaconsulting.sjqs.solutions.DF;

import com.quantiaconsulting.sjqs.solutions.ML.BikeSharing;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.date_format;

public class Preparation_DF_Advanced3 {
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

        //Date Format

        Dataset<Row> dfDF = tempDF
                .groupBy(date_format(col("timestamp"), "E"))
                .sum()
                .select(col("date_format(timestamp, E)").as("dow"), col("sum(requests)").as("total"))
                .orderBy( col("dow"));

        dfDF.show(10);

        spark.stop();
    }
}
