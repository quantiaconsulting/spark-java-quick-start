package com.quantiaconsulting.sjqs.solutions.DF;

import com.quantiaconsulting.sjqs.solutions.ML.BikeSharing;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.date_format;

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

        //dateformat and join

        Dataset<Row> mobileDF = tempDF
                .where("site = 'mobile'")
                .groupBy(date_format(col("timestamp"), "u-E").as("dow"))
                .sum()
                .withColumnRenamed("sum(requests)", "Mobile Total")
                .orderBy( col("dow"));

        Dataset<Row> desktopDF = tempDF
                .where("site = 'desktop'")
                .groupBy(date_format(col("timestamp"), "u-E").as("dow"))
                .sum()                                           // produce the sum of all records
                .withColumnRenamed("sum(requests)", "Desktop Total")
                .orderBy( col("dow"));

        Dataset<Row> jDF = desktopDF.join(mobileDF, "dow");
        jDF.show(10);

        spark.stop();
    }
}
