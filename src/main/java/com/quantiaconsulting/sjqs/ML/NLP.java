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
import java.util.ArrayList;
import java.util.Arrays;


import static org.apache.spark.sql.functions.expr;


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

        reviewsDF.cache();
        reviewsDF.printSchema();
        reviewsDF.show(10);

        // let's have a look to the distribution of rating. How many reviews are there for each rating?

        Dataset<Row> reviewXrating = reviewsDF.groupBy("rating").count();

        reviewXrating.show();

        Dataset<Row> reviewXrating2 = reviewsDF
                .groupBy("rating")
                .agg(expr("count(review) as conteggio"))
                .orderBy("rating");

        reviewXrating2.show();

        // Train-Test Split: Split the original DF into train and test

        Dataset<Row>[] splits = reviewsDF.randomSplit(new double[]{0.8,0.2},42);
        Dataset<Row> trainDF = splits[0];
        Dataset<Row> testDF = splits[1];
        trainDF.cache();
        testDF.cache();

        //Transformers: Create the RegexTokenizer and the StopWordRemover
        RegexTokenizer tokenizer = new RegexTokenizer()
                .setInputCol("review")
                .setOutputCol("tokens")
                .setPattern("\\W+");

        // solo per sapere se stiamo facendo la cosa giusta proviamo ad applicare il tokenizer
        Dataset<Row> tokenizedDF = tokenizer.transform(trainDF.sample(0.01));

        tokenizedDF.show();

       StopWordsRemover remover = new StopWordsRemover()
                .setInputCol("tokens")
                .setOutputCol("stopWordFree");

       String[] stopwords = remover.getStopWords();
       ArrayList<String> stopwordsList = new ArrayList<String>(Arrays.asList(stopwords));
       stopwordsList.add("br");
       String[] newStopWords = new String[stopwordsList.size()];
       newStopWords = stopwordsList.toArray(newStopWords);

       remover = new StopWordsRemover()
                .setInputCol("tokens")
                .setOutputCol("stopWordFree")
               .setStopWords(newStopWords);

        // solo per sapere se stiamo facendo la cosa giusta proviamo ad applicare il remover

        Dataset<Row> removedStopWordsDF = remover.transform(tokenizedDF);

        removedStopWordsDF.show();

        int i = 0;
        for (String s : remover.getStopWords()) {
            System.out.println(s);
            i++;
            if (i>10) break;
        }

        //Estimators: create the CountVectorizer

        CountVectorizer counts = new CountVectorizer()
                .setInputCol("stopWordFree")
                .setOutputCol("features")
                .setVocabSize(1000);

        //create the Binarizer

        Binarizer binarizer = new Binarizer()
                .setInputCol("rating")
                .setOutputCol("label")
                .setThreshold(5);



        Dataset<Row> finalDF = binarizer.transform(removedStopWordsDF);

        finalDF.show();

        // setto label e features anche se non serve perchè abbiamo usato i nomi di default
//
        DecisionTreeClassifier dtc = new DecisionTreeClassifier()
                .setFeaturesCol("features")
                .setLabelCol("label")
                .setMaxDepth(30);

        //Pipeline: Create the ML pipeline
        Pipeline pipeline = new Pipeline()
                .setStages(new PipelineStage[] {tokenizer,remover,counts,binarizer,dtc});
        PipelineModel pipelineModel = pipeline.fit(trainDF);

        Dataset<Row> resultDF = pipelineModel.transform(testDF);

        resultDF.select("review","rating","label","prediction").show();

        resultDF.select("review","rating","label","prediction")
                .groupBy("label","prediction")
                .count()
                .show();

        //Evaluation: Evaluate the results using a MulticlassClassificationEvaluator
        MulticlassClassificationEvaluator evaluator = new MulticlassClassificationEvaluator().setMetricName("accuracy");
        System.out.println("Accuracy:" + evaluator.evaluate(resultDF));

        // ragionamenti su undefitting e overfitting

        Dataset<Row> resultTrainDF = pipelineModel.transform(trainDF);
        System.out.println("Train Accuracy:" + evaluator.evaluate(resultTrainDF));



        spark.stop();

    }

}
