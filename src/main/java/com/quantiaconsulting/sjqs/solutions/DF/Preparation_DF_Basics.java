package com.quantiaconsulting.sjqs.solutions.DF;

import com.quantiaconsulting.sjqs.solutions.ML.BikeSharing;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;

import static org.apache.spark.sql.functions.col;

public class Preparation_DF_Basics {
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

        //DF API
        Dataset<Row> orderedDF = tempDF
                .where( "requests >= 1000" )
                .orderBy(col("requests").desc_nulls_last())
                .select("timestamp", "requests");

        orderedDF.show(10);

        //Temp Table and sql
        tempDF.createOrReplaceTempView("pageViews");
        orderedDF = spark.sql("SELECT timestamp, requests FROM pageViews WHERE requests >= 1000 ORDER BY requests DESC");
        orderedDF.show(10);

        spark.stop();
    }
}
