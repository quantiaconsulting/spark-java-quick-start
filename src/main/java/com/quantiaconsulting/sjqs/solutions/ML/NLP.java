package com.quantiaconsulting.sjqs.solutions.ML;

import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.DecisionTreeClassifier;
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator;
import org.apache.spark.ml.feature.*;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;

public class NLP {
    public static void main(String[] args) {
        String path = NLP.class.getProtectionDomain().getCodeSource().getLocation().getPath();
        String decodedPath = null;
        try {
            decodedPath = URLDecoder.decode(path.substring(0, path.lastIndexOf("/")), "UTF-8");
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }

        SparkSession spark = SparkSession
                .builder()
                .appName("Simple Application")
                .getOrCreate();

        Dataset<Row> reviewsDF = spark
                .read()
                .parquet(decodedPath + "/resources/imdb_ratings_50k.parquet");

        reviewsDF.createOrReplaceTempView("reviews");

        reviewsDF.cache();
        reviewsDF.printSchema();
        reviewsDF.show(10);

        // Train-Test Split
        Dataset<Row>[] splits = reviewsDF.randomSplit(new double[]{0.8, 0.2}, 42);
        Dataset<Row> trainDF = splits[0];
        Dataset<Row> testDF = splits[1];
        trainDF.cache();
        testDF.cache();

        long positiveRatings = trainDF.filter("rating >= 5").count();
        long totalRatings = trainDF.count();
        double baselineAccuracy = ((double) positiveRatings)/totalRatings;

        System.out.println("Baseline accuracy: " + baselineAccuracy);

        //Transformers
        RegexTokenizer tokenizer = new RegexTokenizer()
                .setInputCol("review")
                .setOutputCol("tokens")
                .setPattern("\\W+");

        Dataset<Row> tokenizedDF = tokenizer.transform(trainDF);
        tokenizedDF.show(5);

        StopWordsRemover remover = new StopWordsRemover()
                .setInputCol("tokens")
                .setOutputCol("stopWordFree");

        Dataset<Row> removedStopWordsDF = remover.transform(tokenizedDF);
        removedStopWordsDF.show(5);

        //Estimators
        CountVectorizer counts = new CountVectorizer()
                .setInputCol("stopWordFree")
                .setOutputCol("features")
                .setVocabSize(1000);

        CountVectorizerModel countModel = counts.fit(removedStopWordsDF);

        Binarizer binarizer = new Binarizer()
                .setInputCol("rating")
                .setOutputCol("label")
                .setThreshold(5.0);

        DecisionTreeClassifier dtc = new DecisionTreeClassifier();

        //Pipeline
        Pipeline pipeline = new Pipeline().setStages(new PipelineStage[] {tokenizer, remover, counts, binarizer, dtc});
        PipelineModel pipelineModel = pipeline.fit(trainDF);
        Dataset<Row> resultDF = pipelineModel.transform(testDF);

        //Evaluation
        MulticlassClassificationEvaluator evaluator = new MulticlassClassificationEvaluator().setMetricName("accuracy");
        System.out.println("Accuracy:" + evaluator.evaluate(resultDF));

        spark.stop();

    }

}
