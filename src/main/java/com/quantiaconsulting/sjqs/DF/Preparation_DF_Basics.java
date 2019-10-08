package com.quantiaconsulting.sjqs.DF;

import com.quantiaconsulting.sjqs.ML.BikeSharing;
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

        //DF API: Create a DF :
        //    - select timestamp and requests
        //    - filter out rows with less than 1000 requests
        //    - order by requests (desc)

        //Dataset<Row> orderedDF = <FILL>
        //orderedDF.show(10);

        //Temp Table and sql: Perform the same query in SQL using a temp view

        //tempDF.createOrReplaceTempView<FILL>
        //orderedDF = spark.sql<FILL>
        //orderedDF.show(10);

        spark.stop();
    }
}
