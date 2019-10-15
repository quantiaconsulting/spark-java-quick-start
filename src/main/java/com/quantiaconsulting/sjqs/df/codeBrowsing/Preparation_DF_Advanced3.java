package com.quantiaconsulting.sjqs.df.codeBrowsing;

import com.quantiaconsulting.sjqs.ml.codeBrowsing.BikeSharing;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;

import static org.apache.spark.sql.functions.*;

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
                .option("inferSchema", "true")
                .parquet(parquetFile);

        tempDF.cache();
        tempDF.printSchema();

        //Date Format: Format date column using format "E" (MON, TUE, ecc), group by this new column and sum request
        Dataset<Row> dfDF = tempDF.withColumn("DoW",date_format(col("timestamp"), "E"));
        //Dataset<Row> aggDF = dfDF.groupBy("DoW").sum("requests"); così il nome della colonna è sum(requests)
        Dataset<Row> aggDF = dfDF.groupBy("DoW").agg(expr("sum(requests) as SOMMA"));

        dfDF.show(10);

        aggDF.show(10);

        spark.stop();
    }
}
