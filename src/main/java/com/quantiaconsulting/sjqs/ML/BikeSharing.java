package com.quantiaconsulting.sjqs.ML;

import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.evaluation.RegressionEvaluator;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.feature.VectorIndexer;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.ml.regression.DecisionTreeRegressionModel;
import org.apache.spark.ml.regression.DecisionTreeRegressor;
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

        //Apply split the dataframe in train and test using a 70-30 proportion

        Dataset<Row>[] splits = data.randomSplit(new double[]{0.7,0.3},42);
        Dataset<Row> train = splits[0];
        Dataset<Row> test = splits[1];

//      build the feature vector

        String[] allCols = data.columns();
        String[] cols = Arrays.copyOfRange(allCols,0,allCols.length-1);

        for (String s : cols) { System.out.println(s); }

        VectorAssembler vectorAssembler = new VectorAssembler()
                .setInputCols(cols)
                .setOutputCol("rawFeatures");


        VectorIndexer vectorIndexer = new VectorIndexer()
                .setInputCol("rawFeatures")
                .setOutputCol("features")
                .setMaxCategories(4);

        DecisionTreeRegressor dtr = new DecisionTreeRegressor()
                .setLabelCol("cnt").setMaxDepth(8);

        Pipeline pipeline = new Pipeline().setStages(new PipelineStage[]{vectorAssembler,vectorIndexer,dtr});

        PipelineModel model = pipeline.fit(train);

        Dataset<Row> results = model.transform(test);

        RegressionEvaluator evaluator = new RegressionEvaluator().setLabelCol("cnt");

        double rmse = evaluator.evaluate(results);

        System.out.println("DTR rmse: " + rmse);

        Dataset<Row> resultTrain = model.transform(train);

        double trainRmse = evaluator.evaluate(resultTrain);

        System.out.println("DTR train rmse: " + rmse);

        // proviamo a usare random forest

        RandomForestRegressor rfr = new RandomForestRegressor().setLabelCol("cnt");

        pipeline = new Pipeline().setStages(new PipelineStage[]{vectorAssembler,vectorIndexer,rfr});

        model = pipeline.fit(train);

        results = model.transform(test);

        evaluator = new RegressionEvaluator().setLabelCol("cnt");

        rmse = evaluator.evaluate(results);

        System.out.println("RFR rmse: " + rmse);

        resultTrain = model.transform(train);

        trainRmse = evaluator.evaluate(resultTrain);

        System.out.println("RFR train rmse: " + rmse);

        // proviamo a usare random forest con K-fold cross validation per fare hyper parameter selection

        ParamMap[] paramGrid = new ParamGridBuilder()
                .addGrid(rfr.maxDepth(), new int[]{2,5,10})
                .addGrid(rfr.numTrees(), new int[]{10,50})
                .build();

        CrossValidator cv = new CrossValidator()
                .setEstimator(pipeline)
                .setEvaluator(evaluator)
                .setEstimatorParamMaps(paramGrid)
                .setNumFolds(3)
                .setSeed(42);

        CrossValidatorModel cvModel = cv.fit(train);

        results = cvModel.transform(test);

        rmse = evaluator.evaluate(results);

        System.out.println("Cross Validated RFR rmse:" + rmse);




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
