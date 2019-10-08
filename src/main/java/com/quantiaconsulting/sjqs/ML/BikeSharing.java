package com.quantiaconsulting.sjqs.ML;

import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.evaluation.RegressionEvaluator;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.feature.VectorIndexer;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.ml.regression.RandomForestRegressor;
import org.apache.spark.ml.tuning.CrossValidator;
import org.apache.spark.ml.tuning.CrossValidatorModel;
import org.apache.spark.ml.tuning.ParamGridBuilder;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.Arrays;

public class BikeSharing {
    public static void main(String[] args) {
        String path = BikeSharing.class.getProtectionDomain().getCodeSource().getLocation().getPath();
        String decodedPath = null;
        try {
            decodedPath = URLDecoder.decode(path.substring(0, path.lastIndexOf("/")), "UTF-8");
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        String csvFile = decodedPath + "/resources/Bike-Sharing-Dataset/hour.csv";

        SparkSession spark = SparkSession
                .builder()
                .appName("Simple Application")
                .getOrCreate();

        Dataset<Row> data = spark
                .read()
                .option("sep", ",")
                .option("header",true)
                .option("inferSchema", "true")
                .csv(csvFile)
                .drop("instant", "dteday", "casual", "registered", "holiday", "weekday");

        data.cache();
        data.printSchema();
        data.show(10);

        //Apply Random forest to the bike sharing problem

//        Dataset<Row>[] splits = <FILL>
//        <FILL>
//
//        VectorAssembler vectorAssembler = <FILL>
//        VectorIndexer vectorIndexer = <FILL>
//        RandomForestRegressor rf = <FILL>
//        Pipeline pipeline = <FILL>
//
//        //Create the ParamGrid and evaluate the results
//
//        ParamMap[] paramGrid = <FILL>
//        RegressionEvaluator evaluator = <FILL>
//        CrossValidator cv = <FILL>
//
//       <FILL>
//
//        //evaluation
//        double rmse = evaluator<FILL>
//        System.out.println("Test RMSE = " + rmse);

        spark.stop();

    }

}
