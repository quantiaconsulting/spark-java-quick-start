package com.quantiaconsulting.sjqs.ML;

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

        // Train-Test Split: Split the original DF into train antest
//        Dataset<Row>[] splits = reviewsDF.randomSplit<FILL>
//        Dataset<Row> trainDF = <FILL>
//        Dataset<Row> testDF = <FILL>
//        trainDF.cache();
//        testDF.cache();

        //Transformers: Create the RegexTokenizer and the StopWordRemover
//        RegexTokenizer tokenizer = new RegexTokenizer().<FILL>
//        Dataset<Row> tokenizedDF = tokenizer.<FILL>
//
//        StopWordsRemover remover = new StopWordsRemover().<FILL>
//        Dataset<Row> removedStopWordsDF = remover.<FILL>

        //Estimators: create the CountVectorizer, the binarizer and the DecisionTreeClassifier
//        CountVectorizer counts = new CountVectorizer().<FILL>
//        CountVectorizerModel countModel = counts.<FILL>
//
//        Binarizer binarizer = new Binarizer().<FILL>
//
//        DecisionTreeClassifier dtc = new DecisionTreeClassifier();

        //Pipeline: Create the ML pipeline
//        Pipeline pipeline = new Pipeline().setStages<FILL>
//        PipelineModel pipelineModel = pipeline.<FILL>
//        Dataset<Row> resultDF = pipelineModel.<FILL>

        //Evaluation: Evaluate the results using a MulticlassClassificationEvaluator
//        MulticlassClassificationEvaluator evaluator = new MulticlassClassificationEvaluator()<FILL>
//        System.out.println("Accuracy:" + evaluator.<FILL>);

        spark.stop();

    }

}
