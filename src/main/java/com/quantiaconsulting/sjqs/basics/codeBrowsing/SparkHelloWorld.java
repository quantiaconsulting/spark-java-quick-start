package com.quantiaconsulting.sjqs.basics.codeBrowsing;

import com.quantiaconsulting.sjqs.ml.codeBrowsing.BikeSharing;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;

public class SparkHelloWorld {
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

        Dataset<String> logData = spark
                .read()
                .textFile(logFile)
                .cache();

        long numAs = logData.filter((FilterFunction<String>) s -> s.contains("a")).count();
        long numBs = logData.filter((FilterFunction<String>) s -> s.contains("b")).count();

        System.out.println("Lines with a: " + numAs + ", lines with b: " + numBs);

        spark.stop();
    }
}
