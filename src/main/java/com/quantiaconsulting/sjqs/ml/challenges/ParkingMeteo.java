package com.quantiaconsulting.sjqs.ml.challenges;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;

public class ParkingMeteo {
    public static void main(String[] args) {
        String path = ParkingMeteo.class.getProtectionDomain().getCodeSource().getLocation().getPath();
        String decodedPath = null;
        try {
            decodedPath = URLDecoder.decode(path.substring(0, path.lastIndexOf("/")), "UTF-8");
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        String parquetFile = decodedPath + "/resources/pwJoin.parquet";

        SparkSession spark = SparkSession
                .builder()
                .appName("Simple Application")
                .getOrCreate();

        Dataset<Row> data = spark
                .read()
                .option("sep", ",")
                .option("header",true)
                .option("inferSchema", "true")
                .parquet(parquetFile);

        data.cache();
        data.printSchema();
        data.show(10);

        /*
        Once you created a DataFrame from the parquet try to predict the number of free parking per hour.
            * Use at least two models of your choice
            * Show that you understand hyper-parameter tuning

        Data Description : The parquet file contains data related to a parking in Como and the weather during a period between 01/08/2016 and 01/05/2017.

        Data Schema:
            * temporalId:long
            * parkingId:integer
            * parkingName:string
            * freeParking:double
            * description:string
            * icon:string
            * temperature:double
            * month:integer
            * doy:integer
            * dom:integer
            * dow:string
            * hour:integer
         */

        spark.stop();

    }

}
