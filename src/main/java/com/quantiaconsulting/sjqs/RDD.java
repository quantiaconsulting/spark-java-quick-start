package com.quantiaconsulting.sjqs;

import com.quantiaconsulting.sjqs.ML.BikeSharing;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

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

        SparkConf conf = new SparkConf()
                .setAppName("Simple Application")
                .setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lines = sc.textFile(logFile);

        JavaRDD<Integer> lineLengths = lines.map(s -> s.length());
        int totalLength = lineLengths.reduce((a, b) -> a + b);

        System.out.println("the files contains "+totalLength+" lines");
    }
}
