package com.quantiaconsulting.sjqs.df.codeBrowsing;

import com.quantiaconsulting.sjqs.ml.codeBrowsing.BikeSharing;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;

public class Ingestion_Parquet {
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

        Dataset<Row> tempDF = spark.read().parquet(parquetFile);

        tempDF.printSchema();

        tempDF.show();


        spark.stop();

    }
}
