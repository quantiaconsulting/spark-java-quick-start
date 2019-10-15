package com.quantiaconsulting.sjqs.df.codeBrowsing;

import com.quantiaconsulting.sjqs.ml.codeBrowsing.BikeSharing;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;

import static org.apache.spark.sql.functions.*;

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

        /*Dataset<Row> mobileDF = tempDF
                .withColumn("DoW",date_format(col("timestamp"), "u-E"))
                .groupBy("site","DoW")
                .agg(expr("sum(requests) as SOMMA"),expr("avg(requests) as MEDIA"))
                .orderBy("DoW")
                ;*/

        Dataset<Row> mobileDF = tempDF
                .withColumn("mDoW",date_format(col("timestamp"), "u-E"))
                .groupBy("site","mDoW")
                .agg(expr("sum(requests) as Somma_Mobile"))
                .where("site = 'mobile'")
                ;

        System.out.println("=================== MOBILE ====================");
        //mobileDF.explain();
        //mobileDF.show();

        Dataset<Row> desktopDF = tempDF.where("site = 'desktop'")
                .withColumn("dDoW",date_format(col("timestamp"), "u-E"))
                .groupBy("site","dDoW")
                .agg(expr("sum(requests) as Somma_Desktop"))

                ;

        System.out.println("=================== DESKTOP ====================");
        //desktopDF.explain();
        //desktopDF.show();

        Dataset<Row> jDF = desktopDF
                .join(mobileDF,desktopDF.col("dDoW").equalTo(mobileDF.col("mDoW")))
                .select("dDoW", "Somma_Mobile", "Somma_Desktop")
                .orderBy("dDoW");

        jDF.show(10);

        spark.stop();
    }
}