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


import java.util.Arrays;

public class SomeDataScience {
    public static void main(String[] args) {
        String csvFile = "resources/Bike-Sharing-Dataset/hour.csv";

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

        // ml
        Dataset<Row>[] splits = data.randomSplit(new double[]{0.7, 0.3}, 42);
        Dataset<Row> trainDF = splits[0];
        Dataset<Row> testDF = splits[1];

        String[] cols = data.columns();
        String[] featuresCols = Arrays.copyOfRange(cols,0,cols.length-1);

        VectorAssembler vectorAssembler = new VectorAssembler().setInputCols(featuresCols).setOutputCol("rawFeatures");
        VectorIndexer vectorIndexer = new VectorIndexer().setInputCol("rawFeatures").setOutputCol("features").setMaxCategories(4);
        RandomForestRegressor rf = new RandomForestRegressor().setLabelCol("cnt").setSeed(27);
        Pipeline pipeline = new Pipeline().setStages(new PipelineStage[] {vectorAssembler, vectorIndexer, rf});

        ParamMap[] paramGrid = new ParamGridBuilder()
                .addGrid(rf.maxDepth(), new int[] {2, 5, 10})
                .addGrid(rf.numTrees(), new int[] {10, 50})
                .build();

        RegressionEvaluator evaluator = new RegressionEvaluator()
                .setLabelCol("cnt")
                .setPredictionCol("prediction");

        CrossValidator cv = new CrossValidator()
                .setEstimator(pipeline)
                .setEvaluator(evaluator)
                .setEstimatorParamMaps(paramGrid)
                .setNumFolds(3)
                .setSeed(27);

        //train and test
        CrossValidatorModel cvModel = cv.fit(trainDF);
        Dataset<Row> predictionsDF = cvModel.transform(testDF);
        predictionsDF.select("cnt", "prediction").show();

        //evaluation
        double rmse = evaluator.evaluate(predictionsDF);
        System.out.println("Test RMSE = " + rmse);

        spark.stop();

    }

}
