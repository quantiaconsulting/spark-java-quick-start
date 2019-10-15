package com.quantiaconsulting.sjqs.basics.codeBrowsing;

import com.quantiaconsulting.sjqs.ml.codeBrowsing.BikeSharing;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;

public class RDD {
    public static void main(String[] args) {

        String path = BikeSharing.class.getProtectionDomain().getCodeSource().getLocation().getPath();
        String decodedPath = null;
        try {
            decodedPath = URLDecoder.decode(path.substring(0, path.lastIndexOf("/")), "UTF-8");
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }

        String logFile = decodedPath + "/resources/README.md";

        SparkSession spark = SparkSession
                .builder()
                .appName("Simple Application")
                .getOrCreate();

        JavaRDD<String> lines = spark.read().textFile(logFile).javaRDD();

        JavaRDD<Integer> lineLengths = lines.map(s -> s.length());
        int totalLength = lineLengths.reduce((a, b) -> a + b);

        System.out.println("the files contains "+totalLength+" lines");
    }
}
