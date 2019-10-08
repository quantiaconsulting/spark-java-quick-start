package com.quantiaconsulting.sjqs.DF;

import com.quantiaconsulting.sjqs.ML.BikeSharing;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.date_format;
import static org.apache.spark.sql.functions.unix_timestamp;

public class Preparation_DF_Advanced4 {
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
                .option("inferSchema", "true")
                .parquet(parquetFile);

        tempDF.cache();
        tempDF.printSchema();

        //Date Format: Format date column using format "u-E" (1-MON, 2-TUE, ecc), group by this new column and sum request. Create two DF, one for the mobile site and another for the desktop site

        //Dataset<Row> mobileDF = tempDF.<FILL>
        //Dataset<Row> desktopDF = tempDF.<FILL>

        //Join the previous dataset
        //Dataset<Row> jDF = desktopDF.join<FILL>;
        //jDF.show(10);

        spark.stop();
    }
}
