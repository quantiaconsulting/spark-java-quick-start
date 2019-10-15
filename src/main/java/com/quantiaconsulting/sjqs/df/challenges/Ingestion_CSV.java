package com.quantiaconsulting.sjqs.df.challenges;

import com.quantiaconsulting.sjqs.ml.codeBrowsing.BikeSharing;
import org.apache.spark.sql.SparkSession;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;

public class Ingestion_CSV {
    public static void main(String[] args) {
        String path = BikeSharing.class.getProtectionDomain().getCodeSource().getLocation().getPath();
        String decodedPath = null;
        try {
            decodedPath = URLDecoder.decode(path.substring(0, path.lastIndexOf("/")), "UTF-8");
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        String csvFile = decodedPath + "/resources/2015_02_clickstream.tsv";

        SparkSession spark = SparkSession
                .builder()
                .appName("Simple Application")
                .getOrCreate();

        System.out.println("==================== WITHOUT INFER SCHEMA ====================");

//        Dataset<Row> tempDF1 = spark.read().<FILL>
//        tempDF1.printSchema();
//        tempDF1.show();

        System.out.println("==================== WITH INFER SCHEMA ====================");

//        Dataset<Row> tempDF2 = spark.read().<FILL>
//        tempDF2.printSchema();
//        tempDF2.show();

        System.out.println("==================== WITH SCHEMA ====================");

//        Schema:
//            - prev_id -> int
//            - curr_id -> int
//            - n -> int
//            - prev_title -> string
//            - curr_title -> string
//            - type -> string

//        List<StructField> fields = new ArrayList<>();
//
//        StructField field = <FILL>
//        StructType schema = <FILL>
//
//        Dataset<Row> tempDF3 = spark.read().<FILL>
//        tempDF3.printSchema();
//        tempDF3.show();
        spark.stop();
    }
}
