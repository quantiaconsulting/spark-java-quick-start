package com.quantiaconsulting.sjqs.solutions.ML;

import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.evaluation.RegressionEvaluator;
import org.apache.spark.ml.feature.OneHotEncoderEstimator;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.feature.VectorIndexer;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.ml.regression.LinearRegression;
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

        StringIndexer iWeather = new StringIndexer()
                .setInputCol("icon")
                .setOutputCol("cat_icon")
                .setHandleInvalid("skip");

        StringIndexer iDow = new StringIndexer()
                .setInputCol("dow")
                .setOutputCol("cat_dow")
                .setHandleInvalid("skip");

        OneHotEncoderEstimator oneHotEnc = new OneHotEncoderEstimator()
                .setInputCols(new String[] {"cat_icon", "cat_dow"})
                .setOutputCols(new String[] {"vec_icon", "vec_dow"});

        Dataset<Row>[] splits = data.randomSplit(new double[]{0.8, 0.2}, 273);
        Dataset<Row> trainDF = splits[0];
        Dataset<Row> testDF = splits[1];
        System.out.println(testDF.count() + "," +  trainDF.count());

        String[] featureCols = new String[]{"temperature", "month", "dom", "hour", "vec_icon", "vec_dow"};

        //Linear Regression on freeParking

        VectorAssembler vectorAssembler = new VectorAssembler().setInputCols(featureCols).setOutputCol("features");

        LinearRegression lr = new LinearRegression()
                .setLabelCol("freeParking");

        Pipeline lrPipeline = new Pipeline()
                .setStages(new PipelineStage[] {iWeather, iDow, oneHotEnc, vectorAssembler, lr});
        PipelineModel lrPipelineModel = lrPipeline.fit(trainDF);
        Dataset<Row> predictedDF = lrPipelineModel.transform(testDF);

        RegressionEvaluator lrEvaluator = new RegressionEvaluator()
                .setLabelCol("freeParking")
                .setPredictionCol("prediction");
        double lrRmse = lrEvaluator.setMetricName("rmse").evaluate(predictedDF);
        double lrR2 = lrEvaluator.setMetricName("r2").evaluate(predictedDF);

        System.out.println("Linear Regression prediction evaluation:\nRMSE:" + lrRmse + "\nR2:" + lrR2);

        //Grid Searched Random Forest on freeParking
        RandomForestRegressor rf = new RandomForestRegressor().setLabelCol("freeParking").setSeed(27);

        Pipeline rfPipeline = new Pipeline().setStages(new PipelineStage[] {iWeather, iDow, oneHotEnc, vectorAssembler, rf});

        ParamMap[] paramGrid = new ParamGridBuilder()
                .addGrid(rf.maxDepth(), new int[] {2, 5, 10})
                .addGrid(rf.numTrees(), new int[] {10, 50})
                .build();

        RegressionEvaluator rfEvaluator = new RegressionEvaluator()
                .setLabelCol("freeParking")
                .setPredictionCol("prediction");

        CrossValidator cv = new CrossValidator()
                .setEstimator(rfPipeline)
                .setEvaluator(rfEvaluator)
                .setEstimatorParamMaps(paramGrid)
                .setNumFolds(3)
                .setSeed(27);

        CrossValidatorModel cvModel = cv.fit(trainDF);
        predictedDF = cvModel.bestModel().transform(testDF);

        double rmse = rfEvaluator.setMetricName("rmse").evaluate(predictedDF);
        double r2 = rfEvaluator.setMetricName("r2").evaluate(predictedDF);

        System.out.println("Random Forest prediction evaluation:\nRMSE:" + rmse + "\nR2:" + r2);

        spark.stop();

    }

}
